# -*- coding: utf-8 -*-
import json
import datetime
import scrapy
from urllib import parse
from com.virtual_currency.exchange_adaptee.exchange_adaptee_interface import ExchangeAdapteeInterface, \
    REQUEST_TAG, REQUEST_TAG_KLINE, REQUEST_TAG_MARKET_DEPTH, REQUEST_TAG_TRADE, REQUEST_TAG_LAST_PRICES
from com.virtual_currency.pixiu_adapter.pixiu_target import PIXIU_KLINE_DATA, PIXIU_KLINE_AMOUNT, \
    PIXIU_KLINE_OPEN_PRICE, PIXIU_KLINE_CLOSE_PRICE, PIXIU_KLINE_LOW_PRICE, PIXIU_KLINE_HIGH_PRICE, \
    PIXIU_KLINE_TURNOVER, PIXIU_MARKET_TRADE_DATA, PIXIU_MARKET_TRADE_PRICE, PIXIU_MARKET_TRADE_AMOUNT, \
    PIXIU_MARKET_TRADE_DIRECTION, PIXIU_MARKET_DEPTH_DATA, PIXIU_MARKET_DEPTH_PRICE, PIXIU_MARKET_DEPTH_AMOUNT, \
    PIXIU_MARKET_DEPTH_TYPE, PIXIU_MARKET_TRADE_DIRECTION_BUY, PIXIU_MARKET_TRADE_DIRECTION_SELL, \
    PIXIU_MARKET_DEPTH_TYPE_BID, PIXIU_MARKET_DEPTH_TYPE_ASK, PIXIU_MARKET_DEPTH_RESPONSE_TIME
from com.core.configuration import Configuration
from com.core.logger import Logger
from com.connections.mysql_connection import SQL_TIME_FORMAT


# MARKET_URL = "https://api.huobi.pro"
# TRADE_URL = "https://api.huobi.pro"
MARKET_URL = "https://api.huobi.com"
TRADE_URL = "https://api.huobi.com"
HUOBI_WS = "wss://api.huobi.pro/ws"


HUOBI_MARKET_TRADE_DATA_FORMAT = "%Y-%m-%dT%H:%M:%S.%f"

HUOBI_KLINE_DATA = "id"
HUOBI_KLINE_AMOUNT = "amount"
HUOBI_KLINE_OPEN_PRICE = "open"
HUOBI_KLINE_CLOSE_PRICE = "close"
HUOBI_KLINE_LOW_PRICE = "low"
HUOBI_KLINE_HIGH_PRICE = "high"
HUOBI_KLINE_TURNOVER = "vol"

HUOBI_MARKET_TRADE_DATA = "ts"
HUOBI_MARKET_TRADE_PRICE = "price"
HUOBI_MARKET_TRADE_AMOUNT = "amount"
HUOBI_MARKET_TRADE_DIRECTION =  "direction"

HUOBI_MARKET_TRADE_DIRECTION_BUY = "buy"
HUOBI_MARKET_TRADE_DIRECTION_SELL = "sell"

HUOBI_MARKET_DEPTH_REQUEST_TIME = "request_time"


HUOBI_MARKET_DEPTH_PRICE = 0
HUOBI_MARKET_DEPTH_AMOUNT = 1

HUOBI_MARKET_DEPTH_TYPE_BID = "bids"
HUOBI_MARKET_DEPTH_TYPE_ASK = "asks"


class HuobiAdaptee(ExchangeAdapteeInterface):
    def __init__(self):
        super(HuobiAdaptee, self).__init__()
        self.config = Configuration.get_configuration()
        self.logger = Logger.get_logger()
        self._data_base_name = self.config.mysql_database_name_virtual_currency
        self._mysql_table_name_kline = self.config.mysql_table_name_virtual_currency_huobi_kline
        self._mysql_table_name_market_trade = self.config.mysql_table_name_virtual_currency_huobi_market_trade
        self._mysql_table_name_market_depth = self.config.mysql_table_name_virtual_currency_huobi_market_depth
        self._symbol = "btcusdt"

    def get_kline_request(self):
        # for instance: /market/history/kline?period=1day&size=200&symbol=btcusdt
        params = {'symbol': self._symbol,
                  'period': "1min",
                  'size': 200}

        param_encode = parse.urlencode(params)
        request_url = MARKET_URL + '/market/history/kline' + "?" + param_encode

        meta = {REQUEST_TAG: REQUEST_TAG_KLINE}
        request = scrapy.Request(url=request_url, meta=meta)
        return [request]

    def get_market_depth_request(self):
        # for instance: /market/depth?symbol=ethusdt&type=step1
        # TODO: chang to websock: https://github.com/huobiapi/API_Docs/wiki

        params = {'symbol': self._symbol,
                  'type': "step0"}

        param_encode = parse.urlencode(params)
        request_url = MARKET_URL + '/market/depth' + "?" + param_encode

        now_time = datetime.datetime.strftime(datetime.datetime.now(), SQL_TIME_FORMAT)
        meta = {REQUEST_TAG: REQUEST_TAG_MARKET_DEPTH, HUOBI_MARKET_DEPTH_REQUEST_TIME: now_time}
        request = scrapy.Request(url=request_url, meta=meta)
        return [request]

    def get_market_trade_request(self):
        # for instance: /market/history/trade?symbol=btcusdt
        params = {'symbol': self._symbol,
                  'size': 2000}

        param_encode = parse.urlencode(params)
        request_url = MARKET_URL + '/market/history/trade' + "?" + param_encode

        meta = {REQUEST_TAG: REQUEST_TAG_TRADE}
        request = scrapy.Request(url=request_url, meta=meta)
        return [request]

    def kline_request_callback(self, response) -> list:
        self.logger.debug("Get response: %s tag: %s", response.url, response.meta.get(REQUEST_TAG))
        json_response = json.loads(response.body_as_unicode())
        kline_data_list = list()
        if json_response.get("status") != 'ok':
            return kline_data_list

        data = json_response.get('data')
        for item in data:
            kline_data = dict()
            kline_data[PIXIU_KLINE_DATA] = datetime.datetime.fromtimestamp(int(item.get(HUOBI_KLINE_DATA)))
            kline_data[PIXIU_KLINE_AMOUNT] = item.get(HUOBI_KLINE_AMOUNT)
            kline_data[PIXIU_KLINE_OPEN_PRICE] = item.get(HUOBI_KLINE_OPEN_PRICE)
            kline_data[PIXIU_KLINE_CLOSE_PRICE] = item.get(HUOBI_KLINE_CLOSE_PRICE)
            kline_data[PIXIU_KLINE_LOW_PRICE] = item.get(HUOBI_KLINE_LOW_PRICE)
            kline_data[PIXIU_KLINE_HIGH_PRICE] = item.get(HUOBI_KLINE_HIGH_PRICE)
            kline_data[PIXIU_KLINE_TURNOVER] = item.get(HUOBI_KLINE_TURNOVER)
            kline_data_list.append(kline_data)

        return kline_data_list
        # {'id': 1528308060, 'open': 7519.23, 'close': 7516.15, 'low': 7516.15, 'high': 7519.51, 'amount': 0.7744, 'vol': 5822.028718, 'count': 9}

    def market_trade_request_callback(self, response) -> list:
        self.logger.debug("Get response: %s tag: %s", response.url, response.meta.get(REQUEST_TAG))
        json_response = json.loads(response.body_as_unicode())
        market_trade_data_list = list()
        if json_response.get("status") != 'ok':
            return market_trade_data_list

        data = json_response.get('data')
        for item in data:
            for sub_item in item.get('data'):
                market_trade_data = dict()

                trade_data = str(sub_item.get(HUOBI_MARKET_TRADE_DATA))
                trade_data = float(trade_data[:10] + "." + trade_data[10:])
                trade_data_datetime = datetime.datetime.fromtimestamp(trade_data)
                market_trade_data[PIXIU_MARKET_TRADE_DATA] = trade_data_datetime
                market_trade_data[PIXIU_MARKET_TRADE_PRICE] = sub_item.get(HUOBI_MARKET_TRADE_PRICE)
                market_trade_data[PIXIU_MARKET_TRADE_AMOUNT] = sub_item.get(HUOBI_MARKET_TRADE_AMOUNT)
                if sub_item.get(HUOBI_MARKET_TRADE_DIRECTION) == HUOBI_MARKET_TRADE_DIRECTION_BUY:
                    market_trade_data[PIXIU_MARKET_TRADE_DIRECTION] = PIXIU_MARKET_TRADE_DIRECTION_BUY
                else:
                    market_trade_data[PIXIU_MARKET_TRADE_DIRECTION] = PIXIU_MARKET_TRADE_DIRECTION_SELL

                market_trade_data_list.append(market_trade_data)

        return market_trade_data_list
        # {'id': 7790464886, 'ts': 1526922019737, 'data': [{'amount': 0.001, 'ts': 1526922019737, 'id': 77904648864748937282, 'price': 8387.0, 'direction': 'sell'}]}

    def market_depth_request_callback(self, response) -> list:
        self.logger.debug("Get response: %s tag: %s", response.url, response.meta.get(REQUEST_TAG))
        json_response = json.loads(response.body_as_unicode())
        market_depth_data_list = list()
        if json_response.get("status") != 'ok':
            return market_depth_data_list

        request_time = response.meta.get(HUOBI_MARKET_DEPTH_REQUEST_TIME)
        info_time = datetime.datetime.strptime(request_time, SQL_TIME_FORMAT)
        response_time = datetime.datetime.now()

        data = json_response.get('tick')
        for buy_info in data.get(HUOBI_MARKET_DEPTH_TYPE_BID):
            market_depth_data = dict()
            market_depth_data[PIXIU_MARKET_DEPTH_DATA] = info_time
            market_depth_data[PIXIU_MARKET_DEPTH_RESPONSE_TIME] = response_time
            market_depth_data[PIXIU_MARKET_DEPTH_PRICE] = buy_info[HUOBI_MARKET_DEPTH_PRICE]
            market_depth_data[PIXIU_MARKET_DEPTH_AMOUNT] = buy_info[HUOBI_MARKET_DEPTH_AMOUNT]
            market_depth_data[PIXIU_MARKET_DEPTH_TYPE] = PIXIU_MARKET_DEPTH_TYPE_ASK
            market_depth_data_list.append(market_depth_data)

        for sell_info in data.get(HUOBI_MARKET_DEPTH_TYPE_ASK):
            market_depth_data = dict()
            market_depth_data[PIXIU_MARKET_DEPTH_DATA] = info_time
            market_depth_data[PIXIU_MARKET_DEPTH_RESPONSE_TIME] = response_time
            market_depth_data[PIXIU_MARKET_DEPTH_PRICE] = sell_info[HUOBI_MARKET_DEPTH_PRICE]
            market_depth_data[PIXIU_MARKET_DEPTH_AMOUNT] = sell_info[HUOBI_MARKET_DEPTH_AMOUNT]
            market_depth_data[PIXIU_MARKET_DEPTH_TYPE] = PIXIU_MARKET_DEPTH_TYPE_ASK
            market_depth_data_list.append(market_depth_data)

        return market_depth_data_list
        # [7421.0, 2.3999]

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
    def get_kline_insert_procedure(cls):
       return Configuration.get_configuration().mysql_procedure_insert_huobi_kline

    @classmethod
    def get_market_trade_insert_procedure(cls):
        return Configuration.get_configuration().mysql_procedure_insert_huobi_market_trade


    @classmethod
    def get_market_depth_insert_procedure(cls):
        return Configuration.get_configuration().mysql_procedure_insert_huobi_market_depth
