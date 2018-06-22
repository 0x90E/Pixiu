# -*- coding: utf-8 -*-
import json
import datetime
import scrapy
from urllib import parse
from com.virtual_currency.exchange_adaptee.exchange_adaptee_interface import ExchangeAdapteeInterface, \
    REQUEST_TAG, REQUEST_TAG_KLINE, REQUEST_TAG_MARKET_DEPTH, REQUEST_TAG_TRADE, REQUEST_TAG_LAST_PRICES
from com.core.configuration import Configuration
from com.core.logger import Logger
from com.virtual_currency.pixiu_adapter.pixiu_target import PIXIU_KLINE_DATA, PIXIU_KLINE_AMOUNT, \
    PIXIU_KLINE_OPEN_PRICE, PIXIU_KLINE_CLOSE_PRICE, PIXIU_KLINE_LOW_PRICE, PIXIU_KLINE_HIGH_PRICE, \
    PIXIU_KLINE_TURNOVER, PIXIU_MARKET_TRADE_DATA, PIXIU_MARKET_TRADE_PRICE, PIXIU_MARKET_TRADE_AMOUNT, \
    PIXIU_MARKET_TRADE_DIRECTION, PIXIU_MARKET_DEPTH_DATA, PIXIU_MARKET_DEPTH_PRICE, PIXIU_MARKET_DEPTH_AMOUNT, \
    PIXIU_MARKET_DEPTH_TYPE, PIXIU_MARKET_TRADE_DIRECTION_BUY, PIXIU_MARKET_TRADE_DIRECTION_SELL, \
    PIXIU_MARKET_DEPTH_TYPE_BID, PIXIU_MARKET_DEPTH_TYPE_ASK, PIXIU_MARKET_DEPTH_RESPONSE_TIME
from com.connections.mysql_connection import SQL_TIME_FORMAT

FCOIN_API_V2 = "https://api.fcoin.com/v2/market"

FCOIN_KLINE_DATA = 'id'
FCOIN_KLINE_AMOUNT = 'base_vol'
FCOIN_KLINE_OPEN_PRICE = 'open'
FCOIN_KLINE_CLOSE_PRICE = 'close'
FCOIN_KLINE_LOW_PRICE = 'low'
FCOIN_KLINE_HIGH_PRICE = 'high'
FCOIN_KLINE_TURNOVER = 'quote_vol'

FCOIN_MARKET_TRADE_DATA = "ts"
FCOIN_MARKET_TRADE_PRICE = "price"
FCOIN_MARKET_TRADE_AMOUNT = "amount"
FCOIN_MARKET_TRADE_DIRECTION = "side"

FCOIN_MARKET_TRADE_DIRECTION_BUY = "buy"
FCOIN_MARKET_TRADE_DIRECTION_SELL = "sell"

FCOIN_MARKET_DEPTH_PRICE = 0
FCOIN_MARKET_DEPTH_AMOUNT = 1
FCOIN_MARKET_DEPTH_REQUEST_TIME = "request_time"

FCOIN_MARKET_DEPTH_TYPE_BID = "bids"
FCOIN_MARKET_DEPTH_TYPE_ASK = "asks"


class FcoinAdaptee(ExchangeAdapteeInterface):
    def __init__(self):
        # API doc: https://developer.fcoin.com/zh.html
        super(FcoinAdaptee, self).__init__()
        self.config = Configuration.get_configuration()
        self.logger = Logger.get_logger()
        self._data_base_name = self.config.mysql_database_name_virtual_currency
        self._mysql_table_name_kline = self.config.mysql_table_name_virtual_currency_fcoin_kline
        self._mysql_table_name_market_trade = self.config.mysql_table_name_virtual_currency_fcoin_market_trade
        self._mysql_table_name_market_depth = self.config.mysql_table_name_virtual_currency_fcoin_market_depth
        self._symbol = "btcusdt"

    def get_kline_request(self):
        # for instance: https://api.fcoin.com/v2/market/candles/M1/btcusdt
        request_url = FCOIN_API_V2 + '/candles' + '/M1' + '/' + self._symbol
        meta = {REQUEST_TAG: REQUEST_TAG_KLINE}
        request = scrapy.Request(url=request_url, meta=meta)
        return [request]

    def get_market_trade_request(self):
        # for instance: https://api.fcoin.com/v2/market/trades/btcusdt?limit=1000
        params = {'limit': 1000}
        param_encode = parse.urlencode(params)
        request_url = FCOIN_API_V2 + '/trades' + "/" + self._symbol + "?" + param_encode
        meta = {REQUEST_TAG: REQUEST_TAG_TRADE}
        request = scrapy.Request(url=request_url, meta=meta)
        return [request]

    def get_market_depth_request(self):
        # for instance: https://api.fcoin.com/v2/market/depth/full/btcusdt
        request_url = FCOIN_API_V2 + '/depth' + '/full' + '/' + self._symbol
        now_time = datetime.datetime.strftime(datetime.datetime.now(), SQL_TIME_FORMAT)
        meta = {REQUEST_TAG: REQUEST_TAG_LAST_PRICES, FCOIN_MARKET_DEPTH_REQUEST_TIME: now_time}
        request = scrapy.Request(url=request_url, meta=meta)
        return [request]

    def kline_request_callback(self, response):
        self.logger.debug("Get response: %s tag: %s", response.url, response.meta.get(REQUEST_TAG))
        kline_data_list = list()
        json_response = json.loads(response.body_as_unicode())
        if json_response.get("status") != 0:
            return kline_data_list

        data = json_response.get("data")
        for item in data:
            kline_data = dict()
            trade_data = datetime.datetime.fromtimestamp(item[FCOIN_KLINE_DATA])
            kline_data[PIXIU_KLINE_DATA] = trade_data
            kline_data[PIXIU_KLINE_AMOUNT] = item[FCOIN_KLINE_AMOUNT]
            kline_data[PIXIU_KLINE_OPEN_PRICE] = item[FCOIN_KLINE_OPEN_PRICE]
            kline_data[PIXIU_KLINE_CLOSE_PRICE] = item[FCOIN_KLINE_CLOSE_PRICE]
            kline_data[PIXIU_KLINE_LOW_PRICE] = item[FCOIN_KLINE_LOW_PRICE]
            kline_data[PIXIU_KLINE_HIGH_PRICE] = item[FCOIN_KLINE_HIGH_PRICE]
            kline_data[PIXIU_KLINE_TURNOVER] = item[FCOIN_KLINE_TURNOVER]
            kline_data_list.append(kline_data)

        return kline_data_list

    def market_trade_request_callback(self, response):
        self.logger.debug("Get response: %s tag: %s", response.url, response.meta.get(REQUEST_TAG))
        market_trade_data_list = list()
        json_response = json.loads(response.body_as_unicode())
        if json_response.get("status") != 0:
            return market_trade_data_list

        data = json_response.get("data")
        for item in data:
            market_trade_data = dict()
            str_trade_data = str(item.get(FCOIN_MARKET_TRADE_DATA))
            trade_data = datetime.datetime.fromtimestamp(float(str_trade_data[0:10] + "." + str_trade_data[10:]))
            market_trade_data[PIXIU_MARKET_TRADE_DATA] = trade_data
            market_trade_data[PIXIU_MARKET_TRADE_PRICE] = item.get(FCOIN_MARKET_TRADE_PRICE)
            market_trade_data[PIXIU_MARKET_TRADE_AMOUNT] = item.get(FCOIN_MARKET_TRADE_AMOUNT)
            if item.get(FCOIN_MARKET_TRADE_DIRECTION) == FCOIN_MARKET_TRADE_DIRECTION_BUY:
                market_trade_data[PIXIU_MARKET_TRADE_DIRECTION] = PIXIU_MARKET_TRADE_DIRECTION_BUY
            else:
                market_trade_data[PIXIU_MARKET_TRADE_DIRECTION] = PIXIU_MARKET_TRADE_DIRECTION_SELL

            market_trade_data_list.append(market_trade_data)

        return market_trade_data_list
        # {'amount': 0.0519, 'ts': 1529427969140, 'id': 65188530000, 'side': 'buy', 'price': 6708.84}

    def market_depth_request_callback(self, response) -> list:
        self.logger.debug("Get response: %s tag: %s", response.url, response.meta.get(REQUEST_TAG))
        market_depth_data_list = list()
        json_response = json.loads(response.body_as_unicode())
        if json_response.get("status") != 0:
            return market_depth_data_list

        request_time = response.meta.get(FCOIN_MARKET_DEPTH_REQUEST_TIME)
        info_time = datetime.datetime.strptime(request_time, SQL_TIME_FORMAT)
        response_time = datetime.datetime.now()
        data = json_response.get("data")
        bids = data.get(FCOIN_MARKET_DEPTH_TYPE_BID)
        for i in range(len(bids) // 2):
            market_depth_data = dict()
            market_depth_data[PIXIU_MARKET_DEPTH_DATA] = info_time
            market_depth_data[PIXIU_MARKET_DEPTH_RESPONSE_TIME] = response_time
            market_depth_data[PIXIU_MARKET_DEPTH_PRICE] = bids[2*i + FCOIN_MARKET_DEPTH_PRICE]
            market_depth_data[PIXIU_MARKET_DEPTH_AMOUNT] = bids[2*i + FCOIN_MARKET_DEPTH_AMOUNT]
            market_depth_data[PIXIU_MARKET_DEPTH_TYPE] = PIXIU_MARKET_DEPTH_TYPE_BID
            market_depth_data_list.append(market_depth_data)

        asks = data.get(FCOIN_MARKET_DEPTH_TYPE_ASK)
        for i in range(len(asks) // 2):
            market_depth_data = dict()
            market_depth_data[PIXIU_MARKET_DEPTH_DATA] = info_time
            market_depth_data[PIXIU_MARKET_DEPTH_RESPONSE_TIME] = response_time
            market_depth_data[PIXIU_MARKET_DEPTH_PRICE] = asks[2*i + FCOIN_MARKET_DEPTH_PRICE]
            market_depth_data[PIXIU_MARKET_DEPTH_AMOUNT] = asks[2*i + FCOIN_MARKET_DEPTH_AMOUNT]
            market_depth_data[PIXIU_MARKET_DEPTH_TYPE] = PIXIU_MARKET_DEPTH_TYPE_ASK
            market_depth_data_list.append(market_depth_data)

        return market_depth_data_list
        # [723.42, 0.04062532, 6723.44, 4.5005, 6723.46, 0.104, ...]

    @property
    def kline_table_name(self):
        return self._mysql_table_name_kline

    @property
    def market_trade_table_name(self):
        return self._mysql_table_name_market_trade

    @property
    def market_depth_table_name(self):
        return self._mysql_table_name_market_depth

    @classmethod
    def get_kline_insert_procedure(self):
        return Configuration.get_configuration().mysql_procedure_insert_fcoin_kline

    @classmethod
    def get_market_trade_insert_procedure(self):
        return Configuration.get_configuration().mysql_procedure_insert_fcoin_market_trade

    @classmethod
    def get_market_depth_insert_procedure(self):
        return Configuration.get_configuration().mysql_procedure_insert_fcoin_market_depth