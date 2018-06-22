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

BITTREX_API_V1 = "https://bittrex.com/api/v1.1/public"
BITTREX_API_V2 = "https://bittrex.com/Api/v2.0/pub/market"

BITTREX_KLINE_DATA_FORMAT = "%Y-%m-%dT%H:%M:%S"
BITTREX_MARKET_TRADE_DATA_FORMAT = "%Y-%m-%dT%H:%M:%S.%f"

BITTREX_KLINE_DATA = "T"
BITTREX_KLINE_AMOUNT = "V"
BITTREX_KLINE_OPEN_PRICE = "O"
BITTREX_KLINE_CLOSE_PRICE = "C"
BITTREX_KLINE_LOW_PRICE = "L"
BITTREX_KLINE_HIGH_PRICE = "H"
BITTREX_KLINE_TURNOVER = "BV"

BITTREX_MARKET_TRADE_DATA = "TimeStamp"
BITTREX_MARKET_TRADE_PRICE = "Price"
BITTREX_MARKET_TRADE_AMOUNT = "Quantity"
BITTREX_MARKET_TRADE_DIRECTION =  "OrderType"

BITTREX_MARKET_TRADE_DIRECTION_BUY = "BUY"
BITTREX_MARKET_TRADE_DIRECTION_SELL = "SELL"

BITTREX_MARKET_DEPTH_PRICE = "Rate"
BITTREX_MARKET_DEPTH_AMOUNT = "Quantity"
BITTREX_MARKET_DEPTH_REQUEST_TIME = "request_time"

BITTREX_MARKET_DEPTH_TYPE = "market_depth_type"
BITTREX_MARKET_DEPTH_TYPE_BID = "buy"
BITTREX_MARKET_DEPTH_TYPE_ASK = "sell"


class BittrexAdaptee(ExchangeAdapteeInterface):
    def __init__(self):
        super(BittrexAdaptee, self).__init__()
        self.config = Configuration.get_configuration()
        self.logger = Logger.get_logger()
        self._data_base_name = self.config.mysql_database_name_virtual_currency
        self._mysql_table_name_kline = self.config.mysql_table_name_virtual_currency_bittrex_kline
        self._mysql_table_name_market_trade = self.config.mysql_table_name_virtual_currency_bittrex_market_trade
        self._mysql_table_name_market_depth = self.config.mysql_table_name_virtual_currency_bittrex_market_depth
        self._symbol = "USDT-BTC"

    def get_kline_request(self):
        # for instance: https://bittrex.com/Api/v2.0/pub/market/GetTicks?marketName=USDT-BTC&tickInterval=oneMin
        params = {'marketName': self._symbol,
                  'tickInterval': "oneMin"}

        param_encode = parse.urlencode(params)
        request_url = BITTREX_API_V2 + '/GetTicks' + "?" + param_encode

        meta = {REQUEST_TAG: REQUEST_TAG_KLINE}
        request = scrapy.Request(url=request_url, meta=meta)
        return [request]

    def get_market_trade_request(self):
        # for instance: https://bittrex.com/api/v1.1/public/getmarkethistory?market=USDT-BTC
        params = {'market': self._symbol}

        param_encode = parse.urlencode(params)
        request_url = BITTREX_API_V1 + '/getmarkethistory' + "?" + param_encode

        meta = {REQUEST_TAG: REQUEST_TAG_TRADE}
        request = scrapy.Request(url=request_url, meta=meta)
        return [request]


    def get_market_depth_request(self):
        # for instance: https://bittrex.com/api/v1.1/public/getorderbook?market=BTC-LTC&type=both

        # change to https://bittrex.com/api/v1.1/public/getorderbook?market=USDT-BTC&type=buy
        # https://bittrex.com/api/v1.1/public/getorderbook?market=USDT-BTC&type=sell
        requests = list()
        now_time = datetime.datetime.strftime(datetime.datetime.now(), SQL_TIME_FORMAT)
        meta = {REQUEST_TAG: REQUEST_TAG_LAST_PRICES, BITTREX_MARKET_DEPTH_REQUEST_TIME: now_time,
                BITTREX_MARKET_DEPTH_TYPE: BITTREX_MARKET_DEPTH_TYPE_BID}

        params = {'market': self._symbol,
                  'type': "buy"}
        param_encode = parse.urlencode(params)
        request_url = BITTREX_API_V1 + "/getorderbook" + "?" + param_encode

        requests.append(scrapy.Request(url=request_url, meta=meta))

        meta = {REQUEST_TAG: REQUEST_TAG_LAST_PRICES, BITTREX_MARKET_DEPTH_REQUEST_TIME: now_time,
                BITTREX_MARKET_DEPTH_TYPE: BITTREX_MARKET_DEPTH_TYPE_ASK}
        params = {'market': self._symbol,
                  'type': "sell"}
        param_encode = parse.urlencode(params)
        request_url = BITTREX_API_V1 + "/getorderbook" + "?" + param_encode
        requests.append(scrapy.Request(url=request_url, meta=meta))

        return requests

    def kline_request_callback(self, response):
        self.logger.debug("Get response: %s tag: %s", response.url, response.meta.get(REQUEST_TAG))
        kline_data_list = list()
        json_response = json.loads(response.body_as_unicode())
        if not json_response.get("success"):
            return kline_data_list

        for item in json_response.get('result'):
            kline_data = dict()
            # Bittrex time is GMT time
            kline_data[PIXIU_KLINE_DATA] = \
                datetime.datetime.strptime(item.get(BITTREX_KLINE_DATA), BITTREX_KLINE_DATA_FORMAT) + \
                datetime.timedelta(hours=8)
            kline_data[PIXIU_KLINE_AMOUNT] = item.get(BITTREX_KLINE_AMOUNT)
            kline_data[PIXIU_KLINE_OPEN_PRICE] = item.get(BITTREX_KLINE_OPEN_PRICE)
            kline_data[PIXIU_KLINE_CLOSE_PRICE] = item.get(BITTREX_KLINE_CLOSE_PRICE)
            kline_data[PIXIU_KLINE_LOW_PRICE] = item.get(BITTREX_KLINE_LOW_PRICE)
            kline_data[PIXIU_KLINE_HIGH_PRICE] = item.get(BITTREX_KLINE_HIGH_PRICE)
            kline_data[PIXIU_KLINE_TURNOVER] = item.get(BITTREX_KLINE_TURNOVER)
            kline_data_list.append(kline_data)

        return kline_data_list

    def market_trade_request_callback(self, response):
        self.logger.debug("Get response: %s tag: %s", response.url, response.meta.get(REQUEST_TAG))
        market_trade_data_list = list()
        json_response = json.loads(response.body_as_unicode())
        if not json_response.get("success"):
            return market_trade_data_list

        for item in json_response.get('result'):
            market_trade_data = dict()
            # Bittrex time is GMT time
            trade_data = item.get(BITTREX_MARKET_TRADE_DATA)
            # avoid error format, such as 2018-06-04T15:07:10
            trade_data = trade_data + ".000" if "." not in trade_data else trade_data
            market_trade_data[PIXIU_MARKET_TRADE_DATA] = \
                datetime.datetime.strptime(trade_data, BITTREX_MARKET_TRADE_DATA_FORMAT) + \
                datetime.timedelta(hours=8)
            market_trade_data[PIXIU_MARKET_TRADE_PRICE] = item.get(BITTREX_MARKET_TRADE_PRICE)
            market_trade_data[PIXIU_MARKET_TRADE_AMOUNT] = item.get(BITTREX_MARKET_TRADE_AMOUNT)
            if item.get(BITTREX_MARKET_TRADE_DIRECTION) == BITTREX_MARKET_TRADE_DIRECTION_BUY:
                market_trade_data[PIXIU_MARKET_TRADE_DIRECTION] = PIXIU_MARKET_TRADE_DIRECTION_BUY
            else:
                market_trade_data[PIXIU_MARKET_TRADE_DIRECTION] = PIXIU_MARKET_TRADE_DIRECTION_SELL

            market_trade_data_list.append(market_trade_data)

        return market_trade_data_list
        # {'Id': 51205119, 'TimeStamp': '2018-05-22T15:25:49.387', 'Quantity': 0.005, 'Price': 8244.0, 'Total': 41.22, 'FillType': 'FILL', 'OrderType': 'BUY'}
        # {'Id': 51205099, 'TimeStamp': '2018-05-22T15:24:57.87', 'Quantity': 0.11985846, 'Price': 8244.0, 'Total': 988.11314424, 'FillType': 'FILL', 'OrderType': 'SELL'}

    def market_depth_request_callback(self, response) -> list:
        self.logger.debug("Get response: %s tag: %s", response.url, response.meta.get(REQUEST_TAG))
        market_depth_data_list = list()
        json_response = json.loads(response.body_as_unicode())
        if not json_response.get("success"):
            return market_depth_data_list

        request_time = response.meta.get(BITTREX_MARKET_DEPTH_REQUEST_TIME)
        info_time = datetime.datetime.strptime(request_time, SQL_TIME_FORMAT)
        response_time = datetime.datetime.now()
        data = json_response.get('result')

        if response.meta.get(BITTREX_MARKET_DEPTH_TYPE) == BITTREX_MARKET_DEPTH_TYPE_BID:
            for buy_info in data:
                market_depth_data = dict()
                market_depth_data[PIXIU_MARKET_DEPTH_DATA] = info_time
                market_depth_data[PIXIU_MARKET_DEPTH_RESPONSE_TIME] = response_time
                market_depth_data[PIXIU_MARKET_DEPTH_PRICE] = buy_info.get(BITTREX_MARKET_DEPTH_PRICE)
                market_depth_data[PIXIU_MARKET_DEPTH_AMOUNT] = buy_info.get(BITTREX_MARKET_DEPTH_AMOUNT)
                market_depth_data[PIXIU_MARKET_DEPTH_TYPE] = PIXIU_MARKET_DEPTH_TYPE_BID
                market_depth_data_list.append(market_depth_data)
        elif response.meta.get(BITTREX_MARKET_DEPTH_TYPE) == BITTREX_MARKET_DEPTH_TYPE_ASK:
            for buy_info in data:
                market_depth_data = dict()
                market_depth_data[PIXIU_MARKET_DEPTH_DATA] = info_time
                market_depth_data[PIXIU_MARKET_DEPTH_RESPONSE_TIME] = response_time
                market_depth_data[PIXIU_MARKET_DEPTH_PRICE] = buy_info.get(BITTREX_MARKET_DEPTH_PRICE)
                market_depth_data[PIXIU_MARKET_DEPTH_AMOUNT] = buy_info.get(BITTREX_MARKET_DEPTH_AMOUNT)
                market_depth_data[PIXIU_MARKET_DEPTH_TYPE] = PIXIU_MARKET_DEPTH_TYPE_ASK
                market_depth_data_list.append(market_depth_data)
        else:
            raise NotImplemented

        self.logger.debug("[market_depth_request_callback]%s - %d - %.4f  %.4f",
                          response.meta.get(BITTREX_MARKET_DEPTH_TYPE), len(market_depth_data_list),
                          market_depth_data_list[0].get(PIXIU_MARKET_DEPTH_PRICE), market_depth_data_list[-1].get(PIXIU_MARKET_DEPTH_PRICE))
        return market_depth_data_list
        # {'Quantity': 0.0094992, 'Rate': 8247.0}

    # @property
    # def database_name(self):
    #     return self._data_base_name
    #
    # @property
    # def create_table_statements(self):
    #     create_table_statements = list()
    #
    #     sql_create_kline_table_if_not_exists = 'CREATE TABLE IF NOT EXISTS ' + self._mysql_table_name_kline + ''' (
    #              `id` int(11) NOT NULL AUTO_INCREMENT,
    #              `info_day` char(8) NOT NULL,
    #              `info_hour` char(2) NOT NULL,
    #              `info_min` char(2) NOT NULL,
    #              `amount` double NOT NULL,
    #              `open_price` double NOT NULL,
    #              `close_price` double NOT NULL,
    #              `low_price` double NOT NULL,
    #              `high_price` double NOT NULL,
    #              `turnover` double NOT NULL,
    #              PRIMARY KEY (`id`)
    #             ) ENGINE=InnoDB AUTO_INCREMENT=28 DEFAULT CHARSET=utf8'''
    #
    #     sql_create_market_trade_table_if_not_exists = 'CREATE TABLE IF NOT EXISTS ' + self._mysql_table_name_market_trade + ''' (
    #              `id` int(11) NOT NULL AUTO_INCREMENT,
    #              `info_day` char(8) NOT NULL,
    #              `info_hour` char(2) NOT NULL,
    #              `info_min` char(2) NOT NULL,
    #              `info_sec` char(2) NOT NULL,
    #              `info_msec` char(3) NOT NULL,
    #              `price` double NOT NULL,
    #              `amount` double NOT NULL,
    #              `direction` varchar(20) NOT NULL,
    #              PRIMARY KEY (`id`)
    #             ) ENGINE=InnoDB AUTO_INCREMENT=28 DEFAULT CHARSET=utf8'''
    #
    #     sql_create_market_depth_table_if_not_exists = 'CREATE TABLE IF NOT EXISTS ' + self._mysql_table_name_market_depth + ''' (
    #              `id` int(11) NOT NULL AUTO_INCREMENT,
    #              `info_day` char(8) NOT NULL,
    #              `info_hour` char(2) NOT NULL,
    #              `info_min` char(2) NOT NULL,
    #              `info_sec` char(2) NOT NULL,
    #              `response_day` char(8) NOT NULL,
    #              `response_hour` char(2) NOT NULL,
    #              `response_min` char(2) NOT NULL,
    #              `response_sec` char(2) NOT NULL,
    #              `price` double NOT NULL,
    #              `amount` double NOT NULL,
    #              `type` varchar(20) NOT NULL,
    #              PRIMARY KEY (`id`)
    #             ) ENGINE=InnoDB AUTO_INCREMENT=28 DEFAULT CHARSET=utf8'''
    #
    #     create_table_statements.append(sql_create_kline_table_if_not_exists)
    #     create_table_statements.append(sql_create_market_trade_table_if_not_exists)
    #     create_table_statements.append(sql_create_market_depth_table_if_not_exists)
    #
    #     return create_table_statements

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
        return Configuration.get_configuration().mysql_procedure_insert_bittrex_kline

    @classmethod
    def get_market_trade_insert_procedure(self):
        return Configuration.get_configuration().mysql_procedure_insert_bittrex_market_trade

    @classmethod
    def get_market_depth_insert_procedure(self):
        return Configuration.get_configuration().mysql_procedure_insert_bittrex_market_depth