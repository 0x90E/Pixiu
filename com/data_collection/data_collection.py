# -*- coding: utf-8 -*-
import datetime
from com.connections.mysql_connection import SQL_TIME_FORMAT
from com.legal_tender.pixiu_adapter.pixiu_target import PixiuTarget, PIXIU_ACTION_BUY, PIXIU_ACTION_SELL, PIXIU_FIELD_PRICE


class NumericDataCollection:
    def __init__(self, data_col_name):
        if not isinstance(data_col_name, list):
            raise TypeError("pass a list type as first parameter")

        self._collection = dict.fromkeys(data_col_name, 0)
        self.col_name = data_col_name

    def _avg_prices(self, col_name, number):
        if col_name not in self._collection:
            raise KeyError("There is not %s as col name" % col_name)

        self._collection[col_name] = (self._collection[col_name] + number)/2

    def save_max_target(self, col_name, price, amount, page_number):
        if self._collection[col_name] < price:
            self._collection[col_name] = price
            self._collection[col_name + "_amount"] = amount
            self._collection[col_name + "_page_number"] = page_number
            self._collection[col_name + "_time"] = datetime.datetime.now().strftime(SQL_TIME_FORMAT)

    def save_min_target(self, col_name, price, amount, page_number):
        if self._collection[col_name] > price or self._collection[col_name] == 0:
            self._collection[col_name] = price
            self._collection[col_name + "_amount"] = amount
            self._collection[col_name + "_page_number"] = page_number
            self._collection[col_name + "_time"] = datetime.datetime.now().strftime(SQL_TIME_FORMAT)

    def output_as_csv_format(self):
        result = list()
        for col in self.col_name:
            result.append(str(self._collection.get(col)))

        return ",".join(result)


class NumericSingleValueCollection:
    def __init__(self):
        self.minimal = None
        self.maximal = None

    def save_minimal(self, value):
        if self.maximal is not None:
            raise Exception("Only one value can be save")

        if self.minimal is None:
            self.minimal = value
            return

        if self.minimal > value:
            self.minimal = value

    def save_maximal(self, value):
        if self.minimal is not None:
            raise Exception("Only one value can be save")

        if self.maximal is None:
            self.maximal = value
            return

        if self.maximal > value:
            self.maximal = value

    def clear_value(self):
        self.minimal = None
        self.maximal = None

    @property
    def get_value(self):
        assert not (self.minimal is not None and self.maximal is not None)
        if self.minimal is not None:
            return self.minimal

        if self.maximal is not None:
            return self.minimal

        return None


class TradeCollection:
    def __init__(self):
        self._delimiter = "_"
        self._coin_trade_info_dict = None
        self._init_coin_trade_info_dict()

    def _init_coin_trade_info_dict(self):
        self._coin_trade_info_dict = dict.fromkeys(PixiuTarget.get_pixiu_coin_names())
        for item in self._coin_trade_info_dict:
            action_dict = dict.fromkeys(PixiuTarget.get_pixiu_actions())
            for action_item in action_dict:
                action_dict[action_item] = list()
            self._coin_trade_info_dict[item] = action_dict.copy()

    def add_trade_info(self, coin_name, action, trade_info_dict):
        assert coin_name in PixiuTarget.get_pixiu_coin_names()
        assert action in PixiuTarget.get_pixiu_actions()
        assert isinstance(trade_info_dict, dict)
        for field in PixiuTarget.get_pixiu_trade_field():
            assert field in trade_info_dict
        coin_name_dict = self._coin_trade_info_dict[coin_name]
        assert coin_name_dict[action] is not None
        coin_name_dict[action].append(trade_info_dict)

    def get_all_trade_info(self):
        for trade_info_list in self._get_all_of_trade_info_list():
            for trade_info in trade_info_list:
                yield trade_info

    def get_sell_buy_list_by_coin_name(self, coin_name):
        assert coin_name in PixiuTarget.get_pixiu_coin_names()
        coin_name_dict = self._coin_trade_info_dict[coin_name]
        assert coin_name_dict[PIXIU_ACTION_BUY] is not None
        assert coin_name_dict[PIXIU_ACTION_SELL] is not None

        buy_list = [trade_info.copy() for trade_info in coin_name_dict[PIXIU_ACTION_BUY]]
        sell_list = [trade_info.copy() for trade_info in coin_name_dict[PIXIU_ACTION_SELL]]
        return buy_list, sell_list

    def sort_by_price(self, trade_info):
        return trade_info[PIXIU_FIELD_PRICE]

    def sort_trade_info_by_price(self):
        for trade_info_list in self._get_all_of_trade_info_list():
            trade_info_list.sort(key=self.sort_by_price)

        self._verify_sort_result()

    def _verify_sort_result(self):
        for trade_info_list in self._get_all_of_trade_info_list():
            tmp_value = 0
            for trade_info in trade_info_list:
                assert tmp_value <= trade_info.get(PIXIU_FIELD_PRICE)
                tmp_value = trade_info.get(PIXIU_FIELD_PRICE)

    def _get_all_of_trade_info_list(self):
        for coin_name in PixiuTarget.get_pixiu_coin_names():
            coin_name_dict = self._coin_trade_info_dict[coin_name]
            for action in PixiuTarget.get_pixiu_actions():
                yield coin_name_dict[action]
