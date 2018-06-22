# -*- coding: utf-8 -*-
import datetime
from com.connections.mysql_connection import SQL_TIME_FORMAT_WITHOUT_SECOND
from com.core.logger import Logger


class KlineDataCollection:
    def __init__(self):
        self._kline_data_collection = list()
        self._logger = Logger.get_logger()

    def add_kline_data(self, info_time, amount, open_price, close_price, low_price, high_price, turnover):
        assert isinstance(info_time, datetime.datetime)

        formatted_info_time = datetime.datetime.strftime(info_time, SQL_TIME_FORMAT_WITHOUT_SECOND)
        # the sequence must be the same as db schema
        item = (formatted_info_time, amount, open_price, close_price, low_price, high_price, turnover)
        if item in self._kline_data_collection:
            return False

        self._kline_data_collection.append(item)
        return True

    def get_all_kline_data(self):
        return self._kline_data_collection

    @classmethod
    def filter_kline_data(self, kline_data_collection, time_boundary):
        logger = Logger.get_logger()

        if len(kline_data_collection) == 0:
            logger.debug("Kline data collection is empty")
            return list()

        # sort by time
        sorted_kline_data = \
            sorted(kline_data_collection, key=lambda x: datetime.datetime.strptime(x[0], SQL_TIME_FORMAT_WITHOUT_SECOND))

        formatted_time_boundary= datetime.datetime.strftime(time_boundary, SQL_TIME_FORMAT_WITHOUT_SECOND)
        target_item_index_list = [i for i, item in enumerate(sorted_kline_data) if item[0] == formatted_time_boundary]
        # avoid target_item_index_list is empty, and then make start_item_index will be 0, and return entire kline data
        target_item_index_list.append(-1)
        start_item_index = max(target_item_index_list) + 1

        if start_item_index >= len(sorted_kline_data):
            logger.debug("After time filter[%s][%s], there is no kline data to return", time_boundary,
                         sorted_kline_data[-1][0])
            return list()

        return sorted_kline_data[start_item_index:]