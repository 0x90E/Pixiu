# -*- coding: utf-8 -*-
import datetime
from com.connections.mysql_connection import SQL_TIME_FORMAT_WITH_MILLISECOND, SQL_TIME_FORMAT_MILLISECOND_DELIMITER
from com.core.logger import Logger


class MarketTradeDataCollection:
    def __init__(self):
        self._market_trade_data_collection = list()
        self._logger = Logger.get_logger()

    def add_market_trade_data(self, info_time, price, amount, direction):
        assert isinstance(info_time, datetime.datetime)

        formatted_info_time = datetime.datetime.strftime(info_time, SQL_TIME_FORMAT_WITH_MILLISECOND)
        other_part, millisecond_part = formatted_info_time.split(SQL_TIME_FORMAT_MILLISECOND_DELIMITER)
        # "000" just make sure millisecond_part has at least 3 digits
        millisecond_part = millisecond_part + "000"
        formatted_info_time = other_part + SQL_TIME_FORMAT_MILLISECOND_DELIMITER + millisecond_part[0:3]

        # the sequence must be the same as db schema
        item = (formatted_info_time, price, amount, direction)
        if item in self._market_trade_data_collection:
            return False

        self._market_trade_data_collection.append(item)
        return True

    def get_all_market_trade_data(self):
        return self._market_trade_data_collection

    @classmethod
    def filter_market_trade_data(cla, market_trade_data_collection, time_boundary):
        logger = Logger.get_logger()
        if len(market_trade_data_collection) == 0:
            logger.debug("Market trade data collection is empty")
            return list()

        # sort by time
        sorted_market_trade_data = \
            sorted(market_trade_data_collection, key=lambda x: datetime.datetime.strptime(x[0], SQL_TIME_FORMAT_WITH_MILLISECOND))

        target_item_index_list = [i for i, item in enumerate(sorted_market_trade_data)
                                  if datetime.datetime.strptime(item[0], SQL_TIME_FORMAT_WITH_MILLISECOND) > time_boundary]

        if len(target_item_index_list) == 0:
            logger.debug("After time filter[%s][%s], there is no market trade data to return", time_boundary,
                         sorted_market_trade_data[-1][0])
            return list()

        start_item_index = min(target_item_index_list)
        return sorted_market_trade_data[start_item_index:]