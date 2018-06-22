# -*- coding: utf-8 -*-


class ExchangeAdapteeInterface:
    def __init__(self):
        pass

    def get_trade_field(self):
        pass

    def get_scrapy_start_requests(self):
        pass

    def coin_mapping(self, coin):
        pass

    def action_mapping(self, action):
        pass

    def pay_method_mapping(self, pay_method):
        pass

    def parser_response_and_get_trade_info(self, response):
        pass

    def exchange_pixiu_field_mapping(self, exchange_field):
        pass

    def is_first_page_request_url(self, request_url):
        pass

    def get_coin_name_form_request_url(self, request_url):
        pass

    def get_action_from_request_url(self, request_url):
        pass

    @property
    def get_database_name(self):
        pass
