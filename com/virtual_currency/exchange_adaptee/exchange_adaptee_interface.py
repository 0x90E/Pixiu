# -*- coding: utf-8 -*-
import abc
REQUEST_TAG = "request_tag"
REQUEST_TAG_KLINE = "request_tag_kline"
REQUEST_TAG_MARKET_DEPTH = "request_tag_market_depth"
REQUEST_TAG_TRADE = "request_tag_trade"
REQUEST_TAG_LAST_PRICES = "request_tag_prices_request"


class ExchangeAdapteeInterface(metaclass = abc.ABCMeta):
    def __init__(self):
        self._data_base_name = ""
        self._mysql_table_name_kline = ""
        self._mysql_table_name_market_trade = ""
        self._mysql_table_name_market_depth = ""

    @abc.abstractmethod
    def get_kline_request(self):
        pass

    @abc.abstractmethod
    def get_market_depth_request(self):
        pass

    @abc.abstractmethod
    def get_market_trade_request(self):
        pass

    @abc.abstractmethod
    def kline_request_callback(self, response) -> list:
        pass

    @abc.abstractmethod
    def market_trade_request_callback(self, response) -> list:
        pass

    @abc.abstractmethod
    def market_depth_request_callback(self, response) -> list:
        pass

    @property
    def kline_table_name(self):
        return ""

    @property
    def market_trade_table_name(self):
        return ""

    @property
    def market_depth_table_name(self):
        return ""

    @classmethod
    def get_kline_insert_procedure(cls):
        return  ""

    @classmethod
    def get_market_trade_insert_procedure(cls):
        return ""

    @classmethod
    def get_market_depth_insert_procedure(cls):
        return ""

    @property
    def database_name(self):
        assert self._data_base_name != ""
        return self._data_base_name

    @property
    def create_table_statements(self):
        assert self._mysql_table_name_kline != ""
        assert self._mysql_table_name_market_trade != ""
        assert self._mysql_table_name_market_depth != ""

        create_table_statements = list()

        sql_create_kline_table_if_not_exists = 'CREATE TABLE IF NOT EXISTS ' + self._mysql_table_name_kline + ''' (
                 `id` int(11) NOT NULL AUTO_INCREMENT,
                 `info_day` char(8) NOT NULL,
                 `info_hour` char(2) NOT NULL,
                 `info_min` char(2) NOT NULL,
                 `amount` double NOT NULL,
                 `open_price` double NOT NULL,
                 `close_price` double NOT NULL,
                 `low_price` double NOT NULL,
                 `high_price` double NOT NULL,
                 `turnover` double NOT NULL,
                 PRIMARY KEY (`id`)
                ) ENGINE=InnoDB AUTO_INCREMENT=28 DEFAULT CHARSET=utf8'''

        sql_create_market_trade_table_if_not_exists = 'CREATE TABLE IF NOT EXISTS ' + self._mysql_table_name_market_trade + ''' (
                 `id` int(11) NOT NULL AUTO_INCREMENT,
                 `info_day` char(8) NOT NULL,
                 `info_hour` char(2) NOT NULL,
                 `info_min` char(2) NOT NULL,
                 `info_sec` char(2) NOT NULL,
                 `info_msec` char(3) NOT NULL,
                 `price` double NOT NULL,
                 `amount` double NOT NULL,
                 `direction` varchar(20) NOT NULL,
                 PRIMARY KEY (`id`)
                ) ENGINE=InnoDB AUTO_INCREMENT=28 DEFAULT CHARSET=utf8'''

        sql_create_market_depth_table_if_not_exists = 'CREATE TABLE IF NOT EXISTS ' + self._mysql_table_name_market_depth + ''' (
                 `id` int(11) NOT NULL AUTO_INCREMENT,
                 `info_day` char(8) NOT NULL,
                 `info_hour` char(2) NOT NULL,
                 `info_min` char(2) NOT NULL,
                 `info_sec` char(2) NOT NULL,                 
                 `response_day` char(8) NOT NULL,
                 `response_hour` char(2) NOT NULL,
                 `response_min` char(2) NOT NULL,
                 `response_sec` char(2) NOT NULL,                 
                 `price` double NOT NULL,
                 `amount` double NOT NULL,
                 `type` varchar(20) NOT NULL,
                 PRIMARY KEY (`id`)
                ) ENGINE=InnoDB AUTO_INCREMENT=28 DEFAULT CHARSET=utf8'''

        create_table_statements.append(sql_create_kline_table_if_not_exists)
        create_table_statements.append(sql_create_market_trade_table_if_not_exists)
        create_table_statements.append(sql_create_market_depth_table_if_not_exists)

        return create_table_statements