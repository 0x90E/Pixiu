# -*- coding: utf-8 -*-
import datetime
from com.connections.mysql_connection import SQL_TIME_FORMAT
from com.core.logger import Logger


class MarketDepthDataCollection:
    def __init__(self):
        self._market_depth_data_collection = list()
        self._logger = Logger.get_logger()

    def add_market_depth_data(self, info_time, response_time, price, amount, action_type):
        assert isinstance(info_time, datetime.datetime)
        assert isinstance(response_time, datetime.datetime)

        formatted_info_time = datetime.datetime.strftime(info_time, SQL_TIME_FORMAT)
        formatted_response_time = datetime.datetime.strftime(response_time, SQL_TIME_FORMAT)
        # the sequence must be the same as db schema
        item = (formatted_info_time, formatted_response_time, price, amount, action_type)
        if item in self._market_depth_data_collection:
            return False

        self._market_depth_data_collection.append(item)
        return True

    def get_all_market_depth_data(self):
        if len(self._market_depth_data_collection) == 0:
            self._logger.debug("Market depth data collection is empty")
            return list()

        # sort by time
        sorted_market_depth_data = \
            sorted(self._market_depth_data_collection, key=lambda x: datetime.datetime.strptime(x[0], SQL_TIME_FORMAT))

        return sorted_market_depth_data