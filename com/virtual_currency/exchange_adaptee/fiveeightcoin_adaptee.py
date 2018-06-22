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

FIVEEIGHT_API_V1 = "https://api.58coin.com/v1/spot"

FIVEEIGHT_KLINE_DATA = 0
FIVEEIGHT_KLINE_AMOUNT = 5
FIVEEIGHT_KLINE_OPEN_PRICE = 1
FIVEEIGHT_KLINE_CLOSE_PRICE = 4
FIVEEIGHT_KLINE_LOW_PRICE = 3
FIVEEIGHT_KLINE_HIGH_PRICE = 2
FIVEEIGHT_KLINE_TURNOVER = 6

FIVEEIGHT_MARKET_TRADE_DATA = 0
FIVEEIGHT_MARKET_TRADE_PRICE = 1
FIVEEIGHT_MARKET_TRADE_AMOUNT = 2
FIVEEIGHT_MARKET_TRADE_DIRECTION = 3

FIVEEIGHT_MARKET_TRADE_DIRECTION_BUY = "buy"
FIVEEIGHT_MARKET_TRADE_DIRECTION_SELL = "sell"

FIVEEIGHT_MARKET_DEPTH_PRICE = 0
FIVEEIGHT_MARKET_DEPTH_AMOUNT = 1
FIVEEIGHT_MARKET_DEPTH_REQUEST_TIME = "request_time"

FIVEEIGHT_MARKET_DEPTH_TYPE_BID = "bids"
FIVEEIGHT_MARKET_DEPTH_TYPE_ASK = "asks"


class FiveEightCoinAdaptee(ExchangeAdapteeInterface):
    def __init__(self):
        # API doc: https://github.com/58COIN/58coin-api/blob/master/58coin-docs-ZH-cn.md
        super(FiveEightCoinAdaptee, self).__init__()
        self.config = Configuration.get_configuration()
        self.logger = Logger.get_logger()
        self._data_base_name = self.config.mysql_database_name_virtual_currency
        self._mysql_table_name_kline = self.config.mysql_table_name_virtual_currency_fiveeightcoin_kline
        self._mysql_table_name_market_trade = self.config.mysql_table_name_virtual_currency_fiveeightcoin_market_trade
        self._mysql_table_name_market_depth = self.config.mysql_table_name_virtual_currency_fiveeightcoin_market_depth
        self._symbol = "BTC_USDT"

    def get_kline_request(self):
        # for instance: https://api.58coin.com/v1/spot/candles?symbol=BTC_USDT&period=1min&limit=1000
        params = {'symbol': self._symbol,
                  'period': "1min",
                  'limit': 1000}
        param_encode = parse.urlencode(params)
        request_url = FIVEEIGHT_API_V1 + '/candles' + '?' + param_encode
        meta = {REQUEST_TAG: REQUEST_TAG_KLINE}
        request = scrapy.Request(url=request_url, meta=meta)
        return [request]

    def get_market_trade_request(self):
        # for instance: https://api.58coin.com/v1/spot/trades?symbol=BTC_USDT&limit=500
        params = {'symbol': self._symbol,
                  'limit': 500}
        param_encode = parse.urlencode(params)
        request_url = FIVEEIGHT_API_V1 + '/trades' + "?" + param_encode
        meta = {REQUEST_TAG: REQUEST_TAG_TRADE}
        request = scrapy.Request(url=request_url, meta=meta)
        return [request]

    def get_market_depth_request(self):
        # for instance: https://api.58coin.com/v1/spot/order_book?symbol=BTC_USDT&limit=200
        params = {'symbol': self._symbol,
                  'limit': 2000}
        param_encode = parse.urlencode(params)
        request_url = FIVEEIGHT_API_V1 + '/order_book' + "?" + param_encode
        now_time = datetime.datetime.strftime(datetime.datetime.now(), SQL_TIME_FORMAT)
        meta = {REQUEST_TAG: REQUEST_TAG_LAST_PRICES, FIVEEIGHT_MARKET_DEPTH_REQUEST_TIME: now_time}
        request = scrapy.Request(url=request_url, meta=meta)
        return [request]

    def kline_request_callback(self, response):
        self.logger.debug("Get response: %s tag: %s", response.url, response.meta.get(REQUEST_TAG))
        kline_data_list = list()
        json_response = json.loads(response.body_as_unicode())
        if len(json_response.get('result')) == 0:
            return kline_data_list

        data = json_response.get('result')
        for item in data:
            kline_data = dict()
            str_trade_data = str(item[FIVEEIGHT_KLINE_DATA])
            trade_data = datetime.datetime.fromtimestamp(float(str_trade_data[0:10] + "." + str_trade_data[10:]))
            kline_data[PIXIU_KLINE_DATA] = trade_data
            kline_data[PIXIU_KLINE_AMOUNT] = item[FIVEEIGHT_KLINE_AMOUNT]
            kline_data[PIXIU_KLINE_OPEN_PRICE] = item[FIVEEIGHT_KLINE_OPEN_PRICE]
            kline_data[PIXIU_KLINE_CLOSE_PRICE] = item[FIVEEIGHT_KLINE_CLOSE_PRICE]
            kline_data[PIXIU_KLINE_LOW_PRICE] = item[FIVEEIGHT_KLINE_LOW_PRICE]
            kline_data[PIXIU_KLINE_HIGH_PRICE] = item[FIVEEIGHT_KLINE_HIGH_PRICE]
            kline_data[PIXIU_KLINE_TURNOVER] = item[FIVEEIGHT_KLINE_TURNOVER]
            kline_data_list.append(kline_data)

        return kline_data_list
        # ['1529446080000', '6716.66', '6716.66', '6715.83', '6716.52', '34.73', '234500.433']

    def market_trade_request_callback(self, response):
        self.logger.debug("Get response: %s tag: %s", response.url, response.meta.get(REQUEST_TAG))
        market_trade_data_list = list()
        json_response = json.loads(response.body_as_unicode())
        if len(json_response.get('result')) == 0:
            return market_trade_data_list

        data = json_response.get('result')
        for item in data:
            market_trade_data = dict()
            str_trade_data = str(item[FIVEEIGHT_MARKET_TRADE_DATA])
            trade_data = datetime.datetime.fromtimestamp(float(str_trade_data[0:10] + "." + str_trade_data[10:]))
            market_trade_data[PIXIU_MARKET_TRADE_DATA] = trade_data
            market_trade_data[PIXIU_MARKET_TRADE_PRICE] = item[FIVEEIGHT_MARKET_TRADE_PRICE]
            market_trade_data[PIXIU_MARKET_TRADE_AMOUNT] = (item[FIVEEIGHT_MARKET_TRADE_AMOUNT]).replace("-","")
            if item[FIVEEIGHT_MARKET_TRADE_DIRECTION] == FIVEEIGHT_MARKET_TRADE_DIRECTION_BUY:
                market_trade_data[PIXIU_MARKET_TRADE_DIRECTION] = PIXIU_MARKET_TRADE_DIRECTION_BUY
            else:
                market_trade_data[PIXIU_MARKET_TRADE_DIRECTION] = PIXIU_MARKET_TRADE_DIRECTION_SELL

            market_trade_data_list.append(market_trade_data)

        return market_trade_data_list
        # [1529506840429, '6762.19', '3.307', 'buy']

    def market_depth_request_callback(self, response) -> list:
        self.logger.debug("Get response: %s tag: %s", response.url, response.meta.get(REQUEST_TAG))
        market_depth_data_list = list()
        json_response = json.loads(response.body_as_unicode())
        if len(json_response.get('result')) == 0:
            return market_depth_data_list

        request_time = response.meta.get(FIVEEIGHT_MARKET_DEPTH_REQUEST_TIME)
        info_time = datetime.datetime.strptime(request_time, SQL_TIME_FORMAT)
        response_time = datetime.datetime.now()
        data = json_response.get("result")
        bids = data.get(FIVEEIGHT_MARKET_DEPTH_TYPE_BID)
        for bid in bids:
            market_depth_data = dict()
            market_depth_data[PIXIU_MARKET_DEPTH_DATA] = info_time
            market_depth_data[PIXIU_MARKET_DEPTH_RESPONSE_TIME] = response_time
            market_depth_data[PIXIU_MARKET_DEPTH_PRICE] = bid[FIVEEIGHT_MARKET_DEPTH_PRICE]
            market_depth_data[PIXIU_MARKET_DEPTH_AMOUNT] = bid[FIVEEIGHT_MARKET_DEPTH_AMOUNT]
            market_depth_data[PIXIU_MARKET_DEPTH_TYPE] = PIXIU_MARKET_DEPTH_TYPE_BID
            market_depth_data_list.append(market_depth_data)

        asks = data.get(FIVEEIGHT_MARKET_DEPTH_TYPE_ASK)
        for ask in asks:
            market_depth_data = dict()
            market_depth_data[PIXIU_MARKET_DEPTH_DATA] = info_time
            market_depth_data[PIXIU_MARKET_DEPTH_RESPONSE_TIME] = response_time
            market_depth_data[PIXIU_MARKET_DEPTH_PRICE] = ask[FIVEEIGHT_MARKET_DEPTH_PRICE]
            market_depth_data[PIXIU_MARKET_DEPTH_AMOUNT] = ask[FIVEEIGHT_MARKET_DEPTH_AMOUNT]
            market_depth_data[PIXIU_MARKET_DEPTH_TYPE] = PIXIU_MARKET_DEPTH_TYPE_ASK
            market_depth_data_list.append(market_depth_data)

        return market_depth_data_list
        # [['6380', '0.005'], ['6646.19', '21.967'], ...]

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
        return Configuration.get_configuration().mysql_procedure_insert_fiveeightcoin_kline

    @classmethod
    def get_market_trade_insert_procedure(self):
        return Configuration.get_configuration().mysql_procedure_insert_fiveeightcoin_market_trade

    @classmethod
    def get_market_depth_insert_procedure(self):
        return Configuration.get_configuration().mysql_procedure_insert_fiveeightcoin_market_depth
