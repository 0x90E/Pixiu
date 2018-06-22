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

BINANCE_API_V1 = "https://api.binance.com/api/v1"

BINANCE_KLINE_DATA = 0
BINANCE_KLINE_AMOUNT = 5
BINANCE_KLINE_OPEN_PRICE = 1
BINANCE_KLINE_CLOSE_PRICE = 4
BINANCE_KLINE_LOW_PRICE = 3
BINANCE_KLINE_HIGH_PRICE = 2
BINANCE_KLINE_TURNOVER = 7

BINANCE_MARKET_TRADE_DATA = "time"
BINANCE_MARKET_TRADE_PRICE = "price"
BINANCE_MARKET_TRADE_AMOUNT = "qty"
BINANCE_MARKET_TRADE_DIRECTION = "isBuyerMaker"

BINANCE_MARKET_TRADE_DIRECTION_BUY = False
BINANCE_MARKET_TRADE_DIRECTION_SELL = True

BINANCE_MARKET_DEPTH_PRICE = 0
BINANCE_MARKET_DEPTH_AMOUNT = 1
BINANCE_MARKET_DEPTH_REQUEST_TIME = "request_time"

BINANCE_MARKET_DEPTH_TYPE_BID = "bids"
BINANCE_MARKET_DEPTH_TYPE_ASK = "asks"


class BianceAdaptee(ExchangeAdapteeInterface):
    def __init__(self):
        # API doc: https://github.com/binance-exchange/binance-official-api-docs/blob/master/rest-api.md
        super(BianceAdaptee, self).__init__()
        self.config = Configuration.get_configuration()
        self.logger = Logger.get_logger()
        self._data_base_name = self.config.mysql_database_name_virtual_currency
        self._mysql_table_name_kline = self.config.mysql_table_name_virtual_currency_binance_kline
        self._mysql_table_name_market_trade = self.config.mysql_table_name_virtual_currency_binance_market_trade
        self._mysql_table_name_market_depth = self.config.mysql_table_name_virtual_currency_binance_market_depth
        self._symbol = "BTCUSDT"

    def get_kline_request(self):
        # for instance: https://api.binance.com/api/v1/klines?symbol=BTCUSDT&interval=1m
        params = {'symbol': self._symbol,
                  'interval': "1m"}

        param_encode = parse.urlencode(params)
        request_url = BINANCE_API_V1 + '/klines' + "?" + param_encode

        headers = {
           'User-Agent': 'binance/python',
           # 'X-MBX-APIKEY': ''
        }
        meta = {REQUEST_TAG: REQUEST_TAG_KLINE}
        request = scrapy.Request(url=request_url, headers=headers, meta=meta)
        return [request]

    def get_market_trade_request(self):
        # for instance: https://api.binance.com/api/v1/trades?symbol=BTCUSDT
        params = {'symbol': self._symbol}

        param_encode = parse.urlencode(params)
        request_url = BINANCE_API_V1 + '/trades' + "?" + param_encode

        headers = {
           'User-Agent': 'binance/python',
           # 'X-MBX-APIKEY': ''
        }
        meta = {REQUEST_TAG: REQUEST_TAG_TRADE}
        request = scrapy.Request(url=request_url, headers=headers, meta=meta)
        return [request]

    def get_market_depth_request(self):
        # for instance: https://api.binance.com/api/v1/depth?symbol=BTCUSDT&limit=1000
        params = {'symbol': self._symbol,
                  'limit': "1000"}

        param_encode = parse.urlencode(params)
        request_url = BINANCE_API_V1 + "/depth" + "?" + param_encode

        headers = {
           'User-Agent': 'binance/python',
           # 'X-MBX-APIKEY': ''
        }
        now_time = datetime.datetime.strftime(datetime.datetime.now(), SQL_TIME_FORMAT)
        meta = {REQUEST_TAG: REQUEST_TAG_LAST_PRICES, BINANCE_MARKET_DEPTH_REQUEST_TIME: now_time}
        request = scrapy.Request(url=request_url, headers=headers, meta=meta)
        return [request]

    def kline_request_callback(self, response):
        self.logger.debug("Get response: %s tag: %s", response.url, response.meta.get(REQUEST_TAG))
        kline_data_list = list()
        json_response = json.loads(response.body_as_unicode())
        if len(json_response) == 0:
            return kline_data_list

        for item in json_response:
            kline_data = dict()
            str_trade_data = str(item[BINANCE_KLINE_DATA])
            trade_data = datetime.datetime.fromtimestamp(float(str_trade_data[0:10] + "." + str_trade_data[10:]))
            kline_data[PIXIU_KLINE_DATA] = trade_data
            kline_data[PIXIU_KLINE_AMOUNT] = item[BINANCE_KLINE_AMOUNT]
            kline_data[PIXIU_KLINE_OPEN_PRICE] = item[BINANCE_KLINE_OPEN_PRICE]
            kline_data[PIXIU_KLINE_CLOSE_PRICE] = item[BINANCE_KLINE_CLOSE_PRICE]
            kline_data[PIXIU_KLINE_LOW_PRICE] = item[BINANCE_KLINE_LOW_PRICE]
            kline_data[PIXIU_KLINE_HIGH_PRICE] = item[BINANCE_KLINE_HIGH_PRICE]
            kline_data[PIXIU_KLINE_TURNOVER] = item[BINANCE_KLINE_TURNOVER]
            kline_data_list.append(kline_data)

        return kline_data_list

    def market_trade_request_callback(self, response):
        self.logger.debug("Get response: %s tag: %s", response.url, response.meta.get(REQUEST_TAG))
        market_trade_data_list = list()
        json_response = json.loads(response.body_as_unicode())
        if len(json_response) == 0:
            return market_trade_data_list

        for item in json_response:
            market_trade_data = dict()

            str_trade_data = str(item.get(BINANCE_MARKET_TRADE_DATA))
            trade_data = datetime.datetime.fromtimestamp(float(str_trade_data[0:10] + "." + str_trade_data[10:]))

            market_trade_data[PIXIU_MARKET_TRADE_DATA] = trade_data
            market_trade_data[PIXIU_MARKET_TRADE_PRICE] = item.get(BINANCE_MARKET_TRADE_PRICE)
            market_trade_data[PIXIU_MARKET_TRADE_AMOUNT] = item.get(BINANCE_MARKET_TRADE_AMOUNT)
            if item.get(BINANCE_MARKET_TRADE_DIRECTION) == BINANCE_MARKET_TRADE_DIRECTION_BUY:
                market_trade_data[PIXIU_MARKET_TRADE_DIRECTION] = PIXIU_MARKET_TRADE_DIRECTION_BUY
            else:
                market_trade_data[PIXIU_MARKET_TRADE_DIRECTION] = PIXIU_MARKET_TRADE_DIRECTION_SELL

            market_trade_data_list.append(market_trade_data)

        return market_trade_data_list
        # {'id': 52008406, 'price': '6729.83000000', 'qty': '0.00298400', 'time': 1529419565871, 'isBuyerMaker': False, 'isBestMatch': True}

    def market_depth_request_callback(self, response) -> list:
        self.logger.debug("Get response: %s tag: %s", response.url, response.meta.get(REQUEST_TAG))
        market_depth_data_list = list()
        json_response = json.loads(response.body_as_unicode())
        if json_response.get('lastUpdateId', "") == "":
            return market_depth_data_list

        request_time = response.meta.get(BINANCE_MARKET_DEPTH_REQUEST_TIME)
        info_time = datetime.datetime.strptime(request_time, SQL_TIME_FORMAT)
        response_time = datetime.datetime.now()
        for buy_info in json_response.get(BINANCE_MARKET_DEPTH_TYPE_BID):
            market_depth_data = dict()
            market_depth_data[PIXIU_MARKET_DEPTH_DATA] = info_time
            market_depth_data[PIXIU_MARKET_DEPTH_RESPONSE_TIME] = response_time
            market_depth_data[PIXIU_MARKET_DEPTH_PRICE] = buy_info[BINANCE_MARKET_DEPTH_PRICE]
            market_depth_data[PIXIU_MARKET_DEPTH_AMOUNT] = buy_info[BINANCE_MARKET_DEPTH_AMOUNT]
            market_depth_data[PIXIU_MARKET_DEPTH_TYPE] = PIXIU_MARKET_DEPTH_TYPE_BID
            market_depth_data_list.append(market_depth_data)

        for buy_info in json_response.get(BINANCE_MARKET_DEPTH_TYPE_ASK):
            market_depth_data = dict()
            market_depth_data[PIXIU_MARKET_DEPTH_DATA] = info_time
            market_depth_data[PIXIU_MARKET_DEPTH_RESPONSE_TIME] = response_time
            market_depth_data[PIXIU_MARKET_DEPTH_PRICE] = buy_info[BINANCE_MARKET_DEPTH_PRICE]
            market_depth_data[PIXIU_MARKET_DEPTH_AMOUNT] = buy_info[BINANCE_MARKET_DEPTH_AMOUNT]
            market_depth_data[PIXIU_MARKET_DEPTH_TYPE] = PIXIU_MARKET_DEPTH_TYPE_ASK
            market_depth_data_list.append(market_depth_data)

        return market_depth_data_list
        # ['6749.99000000', '0.00007200', []

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
        return Configuration.get_configuration().mysql_procedure_insert_binance_kline

    @classmethod
    def get_market_trade_insert_procedure(self):
        return Configuration.get_configuration().mysql_procedure_insert_binance_market_trade

    @classmethod
    def get_market_depth_insert_procedure(self):
        return Configuration.get_configuration().mysql_procedure_insert_binance_market_depth