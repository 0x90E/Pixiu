# -*- coding: utf-8 -*-
from com.virtual_currency.pixiu_adapter.pixiu_target import PixiuTarget, PIXIU_KLINE_DATA, PIXIU_KLINE_AMOUNT, \
    PIXIU_KLINE_OPEN_PRICE, PIXIU_KLINE_CLOSE_PRICE, PIXIU_KLINE_LOW_PRICE, PIXIU_KLINE_HIGH_PRICE, \
    PIXIU_KLINE_TURNOVER, PIXIU_MARKET_TRADE_DATA, PIXIU_MARKET_TRADE_PRICE, PIXIU_MARKET_TRADE_AMOUNT, \
    PIXIU_MARKET_TRADE_DIRECTION, PIXIU_MARKET_DEPTH_DATA, PIXIU_MARKET_DEPTH_PRICE, PIXIU_MARKET_DEPTH_AMOUNT, \
    PIXIU_MARKET_DEPTH_TYPE, PIXIU_MARKET_DEPTH_RESPONSE_TIME
from com.data_collection.kline_data_collection import KlineDataCollection
from com.data_collection.market_depth_data_collection import MarketDepthDataCollection
from com.data_collection.market_trade_data_collection import MarketTradeDataCollection
from com.connections.mysql_connection import MysqlConnection


class PixiuAdapter(PixiuTarget):
    def __init__(self, exchange_adaptee):
        super(PixiuAdapter, self).__init__()
        self._exchange_adaptee = exchange_adaptee
        # self.mysql_connection = MysqlConnection.get_mysql_connection()
        # self._create_adapter_database_and_table()
        self._kline_data_collection = KlineDataCollection()
        self._market_depth_collection = MarketDepthDataCollection()
        self._market_trade_collection = MarketTradeDataCollection()

    def get_scrapy_start_requests(self):
        requests = list()
        wrapper_kine_requests = self._callback_wrapper(self._exchange_adaptee.get_kline_request(),
                                                       self.kine_scrapy_requests_callback)
        if wrapper_kine_requests:
            requests += wrapper_kine_requests

        wrapper_market_depth_requests = self._callback_wrapper(self._exchange_adaptee.get_market_depth_request(),
                                                               self.market_depth_scrapy_requests_callback)
        if wrapper_market_depth_requests:
            requests += wrapper_market_depth_requests

        wrapper_market_trade_requests = self._callback_wrapper(self._exchange_adaptee.get_market_trade_request(),
                                                               self.market_trade_scrapy_requests_callback)
        if wrapper_market_trade_requests:
            requests += wrapper_market_trade_requests

        if len(requests) == 0:
            return

        for request in requests:
            yield request

    def _create_adapter_database_and_table(self):
        try:
            self.mysql_connection.create_mysql_database_connection()
            self.mysql_connection.create_and_database(self._exchange_adaptee.database_name)
            self.mysql_connection.use_database(self._exchange_adaptee.database_name)

            create_table_statements = self._exchange_adaptee.create_table_statements
            for create_table_statement in create_table_statements:
                self.mysql_connection.create_table(self._exchange_adaptee.database_name,
                                                   create_table_statement)
        finally:
            self.mysql_connection.close_mysql_database_connection()

    def kine_scrapy_requests_callback(self, response):
        # parser response
        # mapping exchange field to pixiu field
        kline_data_list = self._exchange_adaptee.kline_request_callback(response)
        for kline_data in kline_data_list:
            date = kline_data[PIXIU_KLINE_DATA]
            amount = kline_data[PIXIU_KLINE_AMOUNT]
            open_price = kline_data[PIXIU_KLINE_OPEN_PRICE]
            close_price = kline_data[PIXIU_KLINE_CLOSE_PRICE]
            low_price = kline_data[PIXIU_KLINE_LOW_PRICE]
            high_price = kline_data[PIXIU_KLINE_HIGH_PRICE]
            turnover = kline_data[PIXIU_KLINE_TURNOVER]

            self._kline_data_collection.add_kline_data(date, amount, open_price, close_price, low_price, high_price,
                                                       turnover)

    def market_depth_scrapy_requests_callback(self, response):
        # parser response
        # mapping exchange field to pixiu field
        market_depth_data_list = self._exchange_adaptee.market_depth_request_callback(response)
        for market_depth_data in market_depth_data_list:
            info_time = market_depth_data[PIXIU_MARKET_DEPTH_DATA]
            response_time = market_depth_data[PIXIU_MARKET_DEPTH_RESPONSE_TIME]
            price = market_depth_data[PIXIU_MARKET_DEPTH_PRICE]
            amount = market_depth_data[PIXIU_MARKET_DEPTH_AMOUNT]
            action_type = market_depth_data[PIXIU_MARKET_DEPTH_TYPE]

            self._market_depth_collection.add_market_depth_data(info_time, response_time, price, amount, action_type)

    def market_trade_scrapy_requests_callback(self, response):
        # parser response
        # mapping exchange field to pixiu field
        market_trade_data_list = self._exchange_adaptee.market_trade_request_callback(response)
        for market_trade_data in market_trade_data_list:
            date = market_trade_data[PIXIU_MARKET_TRADE_DATA]
            price = market_trade_data[PIXIU_MARKET_TRADE_PRICE]
            amount = market_trade_data[PIXIU_MARKET_TRADE_AMOUNT]
            direction = market_trade_data[PIXIU_MARKET_TRADE_DIRECTION]

            self._market_trade_collection.add_market_trade_data(date, price, amount, direction)

    @property
    def kline_data_collection(self):
        return self._kline_data_collection.get_all_kline_data()

    @property
    def market_depth_collection(self):
        return self._market_depth_collection.get_all_market_depth_data()

    @property
    def market_trade_collection(self):
        return self._market_trade_collection.get_all_market_trade_data()

    @property
    def kline_table_name(self):
        return self._exchange_adaptee.kline_table_name

    @property
    def trade_table_name(self):
        return self._exchange_adaptee.market_trade_table_name

    def _callback_wrapper(self, scrapy_requests, callback_wrapper):
        # This method will set the callback with callback_wrapper
        if len(scrapy_requests) == 0:
            return None

        for request in scrapy_requests:
            request.callback = callback_wrapper

        return scrapy_requests
