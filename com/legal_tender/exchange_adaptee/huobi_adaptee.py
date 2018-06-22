# -*- coding: utf-8 -*-
import json
import scrapy
from com.legal_tender.exchange_adaptee.exchange_adaptee_interface import ExchangeAdapteeInterface
from com.legal_tender.pixiu_adapter.pixiu_target import PixiuTarget, PIXIU_ACTION_BUY, PIXIU_ACTION_SELL, \
    PIXIU_FIELD_COIN_NAME, PIXIU_FIELD_ACTION, PIXIU_FIELD_PRICE, PIXIU_FIELD_COUNT, PIXIU_FIELD_PAY_METHOD, \
    PIXIU_FIELD_IS_FIXED, PIXIU_FIELD_MIN_TRADE_LIMIT, PIXIU_FIELD_MAX_TRADE_LIMIT, PIXIU_COIN_NAME_BTC, \
    PIXIU_COIN_NAME_USDT, PIXIU_COIN_NAME_ETH, PIXIU_PAY_METHOD_BANK_CARD, PIXIU_PAY_METHOD_ALIPAY, \
    PIXIU_PAY_METHOD_WXPAY, PIXIU_FIELD_USER_ID, PIXIU_PAY_METHOD_PAYPAL, PIXIU_PAY_METHOD_WESTERN_UNION, \
    PIXIU_PAY_METHOD_INTERAC_E_TRANSFER, PIXIU_PAY_METHOD_PAYNOW, PIXIU_FIELD_PAGE
from com.core.logger import Logger
from com.core.configuration import Configuration


HUOBI_API_BTC = "https://otc-api.huobipro.com/v1/otc/trade/list/public?coinId=1"
HUOBI_API_USDT = "https://otc-api.huobipro.com/v1/otc/trade/list/public?coinId=2"
HUOBI_API_ETH = "https://otc-api.huobipro.com/v1/otc/trade/list/public?coinId=3"


HUOBI_ACTION_BUY = "tradeType=1"
HUOBI_ACTION_SELL = "tradeType=0"

HUOBI_FIELD_COIN_ID = "coinId"
HUOBI_FIELD_USER_ID = "userId"
HUOBI_FIELD_ACTION = "tradeType"
HUOBI_FIELD_PRICE = "price"
HUOBI_FIELD_COUNT = "tradeCount"
HUOBI_FIELD_PAY_METHOD = "payMethod"
HUOBI_FIELD_IS_FIXED = "isFixed"
HUOBI_FIELD_MIN_TRADE_LIMIT = "minTradeLimit"
HUOBI_FIELD_MAX_TRADE_LIMIT = "maxTradeLimit"
HUOBI_FIELD_PAGE = "currPage"


class HuobiAdaptee(ExchangeAdapteeInterface):
    def __init__(self):
        self.data_collection = None
        self.logger = Logger.get_logger()
        self.config = Configuration.get_configuration()
        self._database_name = self.config.mysql_database_name_legal_tender_huobi
        self._huobi_table_name = self.config.mysql_table_name_legal_tender_huobi
        self._total_page_statistics_table_name = self.config.mysql_table_name_legal_tender_total_page_statistics

    def get_huobi_trade_field(self):
        return [HUOBI_FIELD_COIN_ID, HUOBI_FIELD_ACTION, HUOBI_FIELD_PRICE, HUOBI_FIELD_COUNT, HUOBI_FIELD_PAY_METHOD,
                HUOBI_FIELD_IS_FIXED, HUOBI_FIELD_MIN_TRADE_LIMIT, HUOBI_FIELD_MAX_TRADE_LIMIT, HUOBI_FIELD_USER_ID,
                HUOBI_FIELD_PAGE]

    def get_scrapy_start_requests(self):
        requests = list()
        for api in self._get_coin_apis():
            api_method = self._api_request_url_mapping(api)
            for action in self._get_actions():
                meta = {"api": api, "action": action}
                request_url = api_method(action, 1)
                requests.append(scrapy.Request(url=request_url,
                                               callback=self.parser_response_and_get_trade_info_for_first_page,
                                               meta=meta))
        return requests

    def coin_mapping(self, coin):
        if coin == 1:
            return PIXIU_COIN_NAME_BTC
        elif coin == 2:
            return PIXIU_COIN_NAME_USDT
        elif coin == 3:
            return PIXIU_COIN_NAME_ETH
        else:
            raise Exception("Unknown api %s" % coin)

    def action_mapping(self, action):
        if action == 1:
            return PIXIU_ACTION_BUY
        elif action == 0:
            return PIXIU_ACTION_SELL
        else:
            raise Exception("Unknown Huobi coin_id %d" % action)

    def pay_method_mapping(self, pay_method):
        if pay_method == '1':
            return PIXIU_PAY_METHOD_BANK_CARD
        elif pay_method == '2':
            return PIXIU_PAY_METHOD_ALIPAY
        elif pay_method == '3':
            return PIXIU_PAY_METHOD_WXPAY
        elif pay_method == '4':
            return PIXIU_PAY_METHOD_PAYPAL
        elif pay_method == '5':
            return PIXIU_PAY_METHOD_WESTERN_UNION
        elif pay_method == '7':
            return  PIXIU_PAY_METHOD_PAYNOW
        elif pay_method == '10':
            return PIXIU_PAY_METHOD_INTERAC_E_TRANSFER
        else:
            raise Exception("Unknown Huobi pay method %s" % pay_method)

    def parser_response_and_get_trade_info_for_first_page(self, response):
        trade_info_list = self.parser_response_and_get_trade_info(response)
        json_response = json.loads(response.body_as_unicode())
        total_page = json_response.get("totalPage")

        if self.config.crawl_mode == 0:
            return trade_info_list, None, total_page

        requests = list()

        if self.config.crawl_mode == 1:
            crawl_max_page = min(self.config.crawl_max_page, total_page)
        else:
            crawl_max_page = total_page

        for page_index in range(2, crawl_max_page+1):
            action = response.meta.get("action")
            api = response.meta.get("api")
            api_method = self._api_request_url_mapping(api)
            request_url = api_method(action, page_index)
            meta = response.meta
            requests.append(scrapy.Request(url=request_url,
                                           callback=self.parser_response_and_get_trade_info_for_others_page, meta=meta))

        return trade_info_list, requests, total_page

    def parser_response_and_get_trade_info_for_others_page(self, response):
        trade_info_list = self.parser_response_and_get_trade_info(response)
        return trade_info_list, None, None

    def parser_response_and_get_trade_info(self, response):
        json_response = json.loads(response.body_as_unicode())
        self._check_json_response(json_response)
        self._check_response(response, json_response)
        trade_info_list = self._get_trade_info(response)

        return trade_info_list

    def is_first_page_request_url(self, request_url):
        return "currPage=1" in request_url.split("&")

    def get_coin_name_from_request_url(self, request_url):
        coin_name_value_list = [coin_name_pair.split("=")[-1] for coin_name_pair in
                                request_url.split("?")[-1].split("&") if HUOBI_FIELD_COIN_ID in coin_name_pair]
        assert len(coin_name_value_list) == 1
        return self.coin_mapping(int(coin_name_value_list[0]))

    def get_action_from_request_url(self, request_url):
        action_value_list = [action_pair.split("=")[-1] for action_pair in request_url.split("&") if
                             HUOBI_FIELD_ACTION in action_pair]

        assert len(action_value_list) == 1
        return self.action_mapping(int(action_value_list[0]))

    def _get_trade_info(self, response):
        json_response = json.loads(response.body_as_unicode())
        data = json_response.get("data")
        trade_info = list()
        trade_info_dict = dict.fromkeys(PixiuTarget.get_pixiu_trade_field())

        for item in data:
            for huobi_field in self.get_huobi_trade_field():
                pixiu_field = self.exchange_pixiu_field_mapping(huobi_field)

                if huobi_field == HUOBI_FIELD_COIN_ID:
                    coin_name = self.coin_mapping(item.get(huobi_field))
                    value = coin_name
                elif huobi_field == HUOBI_FIELD_ACTION:
                    action = self.action_mapping(item.get(huobi_field))
                    value = action
                elif huobi_field == HUOBI_FIELD_PAY_METHOD:
                    pixiu_method_list = [self.pay_method_mapping(method) for method in item.get(huobi_field).split(",")]
                    value = ",".join(filter(None, pixiu_method_list))
                else:
                    value = item.get(huobi_field)

                trade_info_dict[pixiu_field] = value

            trade_info_dict[PIXIU_FIELD_PAGE] = json_response.get(HUOBI_FIELD_PAGE)
            trade_info.append(trade_info_dict.copy())

        return trade_info

    def exchange_pixiu_field_mapping(self, exchange_field):
        if exchange_field == HUOBI_FIELD_COIN_ID:
            return PIXIU_FIELD_COIN_NAME
        elif exchange_field == HUOBI_FIELD_ACTION:
            return PIXIU_FIELD_ACTION
        elif exchange_field == HUOBI_FIELD_PRICE:
            return PIXIU_FIELD_PRICE
        elif exchange_field == HUOBI_FIELD_COUNT:
            return PIXIU_FIELD_COUNT
        elif exchange_field == HUOBI_FIELD_PAY_METHOD:
            return PIXIU_FIELD_PAY_METHOD
        elif exchange_field == HUOBI_FIELD_IS_FIXED:
            return PIXIU_FIELD_IS_FIXED
        elif exchange_field == HUOBI_FIELD_MIN_TRADE_LIMIT:
            return PIXIU_FIELD_MIN_TRADE_LIMIT
        elif exchange_field == HUOBI_FIELD_MAX_TRADE_LIMIT:
            return PIXIU_FIELD_MAX_TRADE_LIMIT
        elif exchange_field == HUOBI_FIELD_USER_ID:
            return PIXIU_FIELD_USER_ID
        elif exchange_field == HUOBI_FIELD_PAGE:
            return PIXIU_FIELD_PAGE
        else:
            raise Exception("Unknown Huobi field")

    def get_user_web_url(self, user_id):
        return "http://otc.huobi.pro/#/checkUser?id={}".format(user_id)

    def _check_json_response(self, json_response):
        if not json_response:
            error_msg = "Can not get json format of response"
            self.logger.error(error_msg)
            raise Exception(error_msg)

        if json_response.get("message") != u"成功":
            error_msg = "Can not get target message"
            self.logger.error(error_msg)
            raise Exception(error_msg)

    def _check_response(self, response, json_response):
        for data in json_response.get("data"):
            coin_id = self._get_coin_id(response.meta.get("api"))
            assert data.get("coinId") == coin_id

    def _get_coin_apis(self):
        return [HUOBI_API_BTC, HUOBI_API_USDT, HUOBI_API_ETH]

    def _get_actions(self):
        return [HUOBI_ACTION_BUY, HUOBI_ACTION_SELL]

    def _api_request_url_mapping(self, api):
        if HUOBI_API_BTC == api:
            return self._get_btc_request_url_by_action_and_curr_page
        elif HUOBI_API_USDT == api:
            return self._get_uedt_request_url_by_action_and_curr_page
        elif HUOBI_API_ETH == api:
            return self._get_eth_request_url_by_action_and_curr_page
        else:
            raise Exception("Unknown api")

    def _get_other_parameter(self, curr_page):
        other_parameter = \
            'country=37&currency=1&payMethod=0&currPage=%d&merchant=1&online=1' % curr_page
        return other_parameter

    def _get_btc_request_url_by_action_and_curr_page(self, action, curr_page):
        other_parameter = self._get_other_parameter(curr_page)
        request_url = "&".join([HUOBI_API_BTC, action, other_parameter])
        return request_url

    def _get_uedt_request_url_by_action_and_curr_page(self, action, curr_page):
        other_parameter = self._get_other_parameter(curr_page)
        request_url = "&".join([HUOBI_API_USDT, action, other_parameter])
        return request_url

    def _get_eth_request_url_by_action_and_curr_page(self, action, curr_page):
        other_parameter = self._get_other_parameter(curr_page)
        request_url = "&".join([HUOBI_API_ETH, action, other_parameter])
        return request_url

    def _get_coin_id(self, api):
        if HUOBI_API_BTC == api:
            return 1
        elif HUOBI_API_USDT == api:
            return 2
        elif HUOBI_API_ETH == api:
            return 3
        else:
            raise Exception("Unknown api")

    def get_insert_trade_info_field(self, crawl_time, coin_name, action, price, amount, pay_method,
                                            min_trade_limit, max_trade_limit, huobi_userid):
        insert_trade_info_field = list()
        insert_trade_info_field.append(self._huobi_table_name)
        insert_trade_info_field.append(crawl_time)
        insert_trade_info_field.append(coin_name)
        insert_trade_info_field.append(action)
        insert_trade_info_field.append(price)
        insert_trade_info_field.append(amount)
        insert_trade_info_field.append(pay_method)
        insert_trade_info_field.append(min_trade_limit)
        insert_trade_info_field.append(max_trade_limit)
        insert_trade_info_field.append(huobi_userid)
        return insert_trade_info_field

    def get_insert_total_page_statistics_statement(self, crawler_day, crawler_hour, crawler_min, coin_name, action,
                                                   total_page):
        sql_insert = \
            "INSERT INTO {} VALUES (id, '{}','{}','{}','{}','{}',{})".format(self._total_page_statistics_table_name,
                                                                             crawler_day, crawler_hour, crawler_min,
                                                                             coin_name, action, total_page)
        return sql_insert

    @property
    def get_database_name(self):
        return self._database_name

    @property
    def get_table_name(self):
        return self._huobi_table_name

    @property
    def get_create_table_statements(self):
        create_table_statements = list()
        sql_create_huobi_table_if_not_exists = 'CREATE TABLE IF NOT EXISTS ' + self._huobi_table_name + ''' (
                 `id` int(11) NOT NULL AUTO_INCREMENT,
                 `crawl_batch` int(11) NOT NULL,
                 `crawl_day` char(8) NOT NULL,
                 `crawl_hour` char(2) NOT NULL,
                 `crawl_min` char(2) NOT NULL,
                 `coin_name` varchar(12) NOT NULL,
                 `action` varchar(4) NOT NULL,
                 `price` float NOT NULL,
                 `amount` float NOT NULL,
                 `pay_method` varchar(20) NOT NULL,
                 `min_trade_limit` int(11) NOT NULL,
                 `max_trade_limit` int(11) NOT NULL,
                 `huobi_userid` varchar(12) NOT NULL,
                 PRIMARY KEY (`id`)
                ) ENGINE=InnoDB AUTO_INCREMENT=28 DEFAULT CHARSET=utf8'''

        sql_create_total_page_statistics_table_if_not_exists =\
            'CREATE TABLE IF NOT EXISTS ' + self._total_page_statistics_table_name + ''' (
                 `id` int(11) NOT NULL AUTO_INCREMENT,
                 `crawl_day` char(8) NOT NULL,
                 `crawl_hour` char(2) NOT NULL,
                 `crawl_min` char(2) NOT NULL,
                 `coin_name` varchar(12) NOT NULL,
                 `action` varchar(4) NOT NULL,
                 `page_number` int(11) NOT NULL,
                 PRIMARY KEY (`id`)
                ) ENGINE=InnoDB AUTO_INCREMENT=28 DEFAULT CHARSET=utf8'''

        create_table_statements.append(sql_create_huobi_table_if_not_exists)
        create_table_statements.append(sql_create_total_page_statistics_table_if_not_exists)

        return create_table_statements
