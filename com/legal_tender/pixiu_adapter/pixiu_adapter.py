# -*- coding: utf-8 -*-
import os
import datetime
from com.legal_tender.pixiu_adapter.pixiu_target import PixiuTarget, PIXIU_FIELD_COIN_NAME, PIXIU_FIELD_ACTION, \
    PIXIU_FIELD_PRICE, PIXIU_FIELD_CRAWLER_TIME, PIXIU_FIELD_COUNT, PIXIU_FIELD_PAY_METHOD, PIXIU_FIELD_MIN_TRADE_LIMIT, \
    PIXIU_FIELD_MAX_TRADE_LIMIT, PIXIU_FIELD_USER_ID, PIXIU_FIELD_PAGE
from com.data_collection.data_collection import TradeCollection
from com.connections.mysql_connection import MysqlConnection, SQL_TIME_FORMAT
from com.data_collection.mail_content_collection import MailContentCollection
from com.connections.mail_server_connection import MailServerConnection


class PixiuAdapter(PixiuTarget):
    def __init__(self, exchange_adaptee):
        super(PixiuAdapter, self).__init__()
        self._trade_collection = TradeCollection()
        self._exchange_adaptee = exchange_adaptee
        self._request_index = 0
        self._callback_dict = dict()
        self.mysql_connection = MysqlConnection.get_mysql_connection()
        self._create_adapter_database_and_table()

    def get_scrapy_start_requests(self):
        scrapy_start_requests = self._exchange_adaptee.get_scrapy_start_requests()

        if scrapy_start_requests is None or len(scrapy_start_requests) == 0:
            return

        self._callback_wrapper(scrapy_start_requests)

        for request in scrapy_start_requests:
            yield request

    def scrapy_requests_callback(self, response):
        uuid = response.meta.get("uuid")
        parser_response_and_get_trade_info = self._callback_dict.pop(uuid)
        # parser response
        # mapping exchange field to pixiu field
        trade_info_list, scrapy_requests, total_page = parser_response_and_get_trade_info(response)
        coin_name = self._exchange_adaptee.get_coin_name_from_request_url(response.request.url)
        action = self._exchange_adaptee.get_action_from_request_url(response.request.url)
        now_time = datetime.datetime.now()
        for trade_info in trade_info_list:
            # inspect values of coin_name and action is all the same in trade_info_list
            assert coin_name == trade_info[PIXIU_FIELD_COIN_NAME]
            assert action == trade_info[PIXIU_FIELD_ACTION]
            trade_info[PIXIU_FIELD_CRAWLER_TIME] = now_time.strftime(SQL_TIME_FORMAT)

            self._trade_collection.add_trade_info(coin_name, action, trade_info)

        self._check_arbitrage_and_send_mail(response.request.url, coin_name)

        del parser_response_and_get_trade_info

        if total_page is not None:
            self._insert_total_page_into_mysql(now_time, coin_name, action, total_page)

        if scrapy_requests is not None:
            self._callback_wrapper(scrapy_requests)
            for scrapy_request in scrapy_requests:
                yield scrapy_request

    def save_trade_data_into_mysql(self):
        try:
            self.mysql_connection.create_mysql_database_connection()
            self.mysql_connection.use_database(self._exchange_adaptee.get_database_name)
            insert_trade_info_sql_statements = list()
            for trade_info in self._trade_collection.get_all_trade_info():
                self.logger.info(trade_info)

                crawl_time = trade_info[PIXIU_FIELD_CRAWLER_TIME]
                coin_name = trade_info[PIXIU_FIELD_COIN_NAME]
                action = trade_info[PIXIU_FIELD_ACTION]
                price = trade_info[PIXIU_FIELD_PRICE]
                amount = trade_info[PIXIU_FIELD_COUNT]
                pay_method = trade_info[PIXIU_FIELD_PAY_METHOD]
                min_trade_limit = trade_info[PIXIU_FIELD_MIN_TRADE_LIMIT]
                max_trade_limit = trade_info[PIXIU_FIELD_MAX_TRADE_LIMIT]
                huobi_userid = trade_info[PIXIU_FIELD_USER_ID]

                trade_info_field = \
                    self._exchange_adaptee.get_insert_trade_info_field(crawl_time, coin_name, action, price,
                                                                       amount, pay_method, min_trade_limit,
                                                                       max_trade_limit, huobi_userid)
                insert_trade_info_sql_statements.append(trade_info_field)

            procedure_name = self.config.mysql_procedure_insert_legal_tender_data
            self.mysql_connection.insert_trade_data_by_procedure(procedure_name, insert_trade_info_sql_statements)
            self.logger.info("[%d]total of trade info %d" % (os.getpid(), len(insert_trade_info_sql_statements)))
        finally:
            self.mysql_connection.close_mysql_database_connection()

    def _check_arbitrage_and_send_mail(self, request_url, coin_name):
        # this condition can make sure all kind of coin only be checked once
        if not self._exchange_adaptee.is_first_page_request_url(request_url):
            return

        buy_trade_info_list, sell_trade_info_list = self._trade_collection.get_sell_buy_list_by_coin_name(coin_name)
        if len(buy_trade_info_list) == 0 or len(sell_trade_info_list) == 0:
            return

        first_page_buy_trade_info_list = \
            [trade_info.copy() for trade_info in buy_trade_info_list if trade_info.get(PIXIU_FIELD_PAGE) == 1]
        first_page_sell_trade_info_list = \
            [trade_info.copy() for trade_info in sell_trade_info_list if trade_info.get(PIXIU_FIELD_PAGE) == 1]

        first_page_buy_trade_info_list.sort(key=self._trade_collection.sort_by_price)
        first_page_sell_trade_info_list.sort(key=self._trade_collection.sort_by_price)
        first_page_sell_trade_info_list.reverse()

        test_buy_price = None
        test_sell_price = None
        # verify sort result
        range_end = min([3, len(first_page_buy_trade_info_list), len(first_page_sell_trade_info_list)])
        for i in range(0, range_end):
            buy_price = first_page_buy_trade_info_list[i].get(PIXIU_FIELD_PRICE)
            sell_price = first_page_sell_trade_info_list[i].get(PIXIU_FIELD_PRICE)

            test_buy_price = buy_price if test_buy_price is None else test_buy_price
            test_sell_price = sell_price if test_sell_price is None else test_sell_price
            assert test_buy_price <= buy_price
            assert test_sell_price >= sell_price
            test_buy_price = buy_price
            test_sell_price = sell_price


        mail_content_collection = MailContentCollection()
        need_send_mail = False
        # get the first trade info
        buy_price = first_page_buy_trade_info_list[0].get(PIXIU_FIELD_PRICE)
        sell_price = first_page_sell_trade_info_list[0].get(PIXIU_FIELD_PRICE)
        user_id = first_page_buy_trade_info_list[0].get(PIXIU_FIELD_USER_ID)
        user_web_url = self._exchange_adaptee.get_user_web_url(user_id)

        if buy_price < sell_price:
            need_send_mail = True
            buy_amount = first_page_buy_trade_info_list[0].get(PIXIU_FIELD_COUNT)
            sell_amount = first_page_sell_trade_info_list[0].get(PIXIU_FIELD_COUNT)
            buy_min_trade_limit = first_page_buy_trade_info_list[0].get(PIXIU_FIELD_MIN_TRADE_LIMIT)
            mail_content_collection.add_arbitrage_part(coin_name, buy_price, sell_price, buy_amount, sell_amount,
                                                       buy_min_trade_limit, user_web_url)

            # append the second and 3th trade info
            range_end = min([3, len(first_page_buy_trade_info_list), len(first_page_sell_trade_info_list)])
            for i in range(1, range_end):
                buy_price = first_page_buy_trade_info_list[i].get(PIXIU_FIELD_PRICE)
                sell_price = first_page_sell_trade_info_list[i].get(PIXIU_FIELD_PRICE)
                user_id = first_page_buy_trade_info_list[i].get(PIXIU_FIELD_USER_ID)
                user_web_url = self._exchange_adaptee.get_user_web_url(user_id)

                buy_amount = first_page_buy_trade_info_list[i].get(PIXIU_FIELD_COUNT)
                sell_amount = first_page_sell_trade_info_list[i].get(PIXIU_FIELD_COUNT)
                buy_min_trade_limit = first_page_buy_trade_info_list[i].get(PIXIU_FIELD_MIN_TRADE_LIMIT)
                mail_content_collection.add_arbitrage_part(coin_name, buy_price, sell_price, buy_amount, sell_amount,
                                                           buy_min_trade_limit, user_web_url)

        if need_send_mail:
            main_server_connection = MailServerConnection()
            subject = mail_content_collection.get_arbitrage_mail_subject()
            main_server_connection.send_mail(subject, mail_content_collection.get_mail_content())
            del main_server_connection

        del mail_content_collection
        del buy_trade_info_list
        del sell_trade_info_list
        del first_page_buy_trade_info_list
        del first_page_sell_trade_info_list

    def _create_adapter_database_and_table(self):
        try:
            self.mysql_connection.create_mysql_database_connection()
            self.mysql_connection.create_and_database(self._exchange_adaptee.get_database_name)
            self.mysql_connection.use_database(self._exchange_adaptee.get_database_name)

            create_table_statements = self._exchange_adaptee.get_create_table_statements
            for create_table_statement in create_table_statements:
                self.mysql_connection.create_table(self._exchange_adaptee.get_database_name,
                                                   create_table_statement)
        finally:
            self.mysql_connection.close_mysql_database_connection()

    def _insert_total_page_into_mysql(self, now_time, coin_name, action, total_page):
        crawler_day = "".join([str(now_time.year), str(now_time.month), str(now_time.day)])
        crawler_hour = str(now_time.hour)
        crawler_min = str(now_time.minute)
        try:
            self.mysql_connection.create_mysql_database_connection()
            self.mysql_connection.use_database(self._exchange_adaptee.get_database_name)
            sql_insert = \
                self._exchange_adaptee.get_insert_total_page_statistics_statement(crawler_day, crawler_hour,
                                                                                  crawler_min, coin_name, action,
                                                                                  total_page)
            self.logger.debug("Insert total page statistics %s" % sql_insert)
            self.mysql_connection.insert(sql_insert)
        finally:
            self.mysql_connection.close_mysql_database_connection()

    def _callback_wrapper(self, scrapy_requests):
        # This method will replace the callback with self.scrapy_requests_callback,
        # and save the origin callback method into self._callback_dict
        # when get the response, use uuid to get the origin callback

        for request in scrapy_requests:
            if request.callback is not None and not callable(request.callback):
                raise Exception("Is not callable object")

            uuid = self._get_unique_request_index()
            request.meta["uuid"] = uuid
            self._callback_dict[uuid] = request.callback
            request.callback = self.scrapy_requests_callback

    def _get_unique_request_index(self):
        now_index = self._request_index
        self._request_index = now_index + 1
        return now_index
