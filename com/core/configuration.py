# -*- coding: utf-8 -*-
import os
import datetime
import copy
import configparser
import logging
from com.core.configuration_define import *
from com.core.singleton import singleton


@singleton
class ConfigurationSingleton:
    def __init__(self, config_file, config_args):
        print("[%d]ConfigurationSingleton only init once" % os.getpid())
        self._config_file = config_file
        self._log_file = datetime.datetime.now().strftime("%Y%m%d_%H%M%S") + ".log"
        self._log_file_root_dir = ""
        self._proxy = ""
        self._summary_period = 0
        self._low_disk_space_alert = 0.0
        self._low_disk_space_terminal = 0.0
        self._mysql_ip = ""
        self._mysql_port = 0
        self._mysql_user = ""
        self._mysql_pwd = ""
        self._mysql_database_name_legal_tender_huobi = ""
        self._mysql_table_name_legal_tender_huobi = ""
        self._mysql_table_name_legal_tender_total_page_statistics = ""

        self._mysql_database_name_virtual_currency = ""
        self._mysql_table_name_virtual_currency_huobi_kline = ""
        self._mysql_table_name_virtual_currency_huobi_market_trade = ""
        self._mysql_table_name_virtual_currency_huobi_market_depth = ""
        self._mysql_table_name_virtual_currency_bittrex_kline = ""
        self._mysql_table_name_virtual_currency_bittrex_market_trade = ""
        self._mysql_table_name_virtual_currency_bittrex_market_depth = ""

        self._mysql_table_name_virtual_currency_binance_kline = ""
        self._mysql_table_name_virtual_currency_binance_market_trade = ""
        self._mysql_table_name_virtual_currency_binance_market_depth = ""

        self._mysql_table_name_virtual_currency_fcoin_kline = ""
        self._mysql_table_name_virtual_currency_fcoin_market_trade = ""
        self._mysql_table_name_virtual_currency_fcoin_market_depth = ""

        self._mysql_table_name_virtual_currency_fiveeightcoin_kline = ""
        self._mysql_table_name_virtual_currency_fiveeightcoin_market_trade = ""
        self._mysql_table_name_virtual_currency_fiveeightcoin_market_depth = ""


        self._mysql_procedure_insert_legal_tender_data = ""
        self._mysql_procedure_insert_bittrex_market_depth = ""
        self._mysql_procedure_insert_bittrex_market_trade = ""
        self._mysql_procedure_insert_bittrex_kline = ""

        self._mysql_procedure_insert_huobi_market_depth = ""
        self._mysql_procedure_insert_huobi_market_trade = ""
        self._mysql_procedure_insert_huobi_kline = ""


        self._mysql_procedure_insert_binance_market_depth = ""
        self._mysql_procedure_insert_binance_market_trade = ""
        self._mysql_procedure_insert_binance_kline = ""

        self._mysql_procedure_insert_fcoin_market_depth = ""
        self._mysql_procedure_insert_fcoin_market_trade = ""
        self._mysql_procedure_insert_fcoin_kline = ""

        self._mysql_procedure_insert_fiveeightcoin_market_depth = ""
        self._mysql_procedure_insert_fiveeightcoin_market_trade = ""
        self._mysql_procedure_insert_fiveeightcoin_kline = ""

        self._mysql_procedure_get_max_time_market_trade = ""
        self._mysql_procedure_get_max_time_kline = ""
        self._crawl_period = 0
        self._sell_interval_multiple_threshold = 0.0
        self._buy_interval_multiple_threshold = 0.0
        self._mail_smtp_domain = ""
        self.__mail_smtp_port = ""
        self._mail_login_user = ""
        self._mail_login_pwd = ""
        self._mail_address = list()
        self._crawl_mode = 0
        self._crawl_max_page = 0

        self.configs = self._init_configs()
        self.update(config_file, config_args)
        self._set_properties()

    def update(self, config_file="", config_args=None):
        if config_file != "":
            self._update_by_file(config_file)

        # if config_args is not None:
        #     self._update_by_memory(config_args)

    def show(self, output=None):
        if isinstance(output, logging.Logger):
            output.info("====== Configurations ======")
            for key in self.configs:
                output.info("[%s]", key)
                for item in self.configs[key]:
                    output.info("    %s: %s", item[0], item[1])
            output.info("---------------------------")
            output.info("Proxy: %s ", self.proxy)
            output.info("MySQL ip: %s ", self._mysql_ip)
            output.info("MySQL port: %d ", self._mysql_port)
            output.info("MySQL user: %s ", self._mysql_user)
            output.info("MySQL pwd: %s ", self._mysql_pwd)
            output.info("MySQL database name legal tender huobi: %s ", self._mysql_database_name_legal_tender_huobi)
            output.info("MySQL table name legal tender huobi: %s ", self._mysql_table_name_legal_tender_huobi)
            output.info("MySQL table name legal tender total page statistics: %s ", self._mysql_table_name_legal_tender_total_page_statistics)
            output.info("MySQL database name virtual currency: %s ", self._mysql_database_name_virtual_currency)
            output.info("MySQL table name virtual currency huobi kline: %s ", self._mysql_table_name_virtual_currency_huobi_kline)
            output.info("MySQL table name virtual currency huobi market trade: %s ", self._mysql_table_name_virtual_currency_huobi_market_trade)
            output.info("MySQL table name virtual currency huobi market depth: %s ", self._mysql_table_name_virtual_currency_huobi_market_depth)
            output.info("MySQL table name virtual currency bittrex kline: %s ", self._mysql_table_name_virtual_currency_bittrex_kline)
            output.info("MySQL table name virtual currency bittrex market trade: %s ", self._mysql_table_name_virtual_currency_bittrex_market_trade)
            output.info("MySQL table name virtual currency bittrex market depth: %s ", self._mysql_table_name_virtual_currency_bittrex_market_depth)
            output.info("MySQL table name virtual currency binance kline: %s ", self._mysql_table_name_virtual_currency_binance_kline)
            output.info("MySQL table name virtual currency binance market trade: %s ", self._mysql_table_name_virtual_currency_binance_market_trade)
            output.info("MySQL table name virtual currency binance market depth: %s ", self._mysql_table_name_virtual_currency_binance_market_depth)
            output.info("MySQL table name virtual currency fcoin kline: %s ", self._mysql_table_name_virtual_currency_fcoin_kline)
            output.info("MySQL table name virtual currency fcoin market trade: %s ", self._mysql_table_name_virtual_currency_fcoin_market_trade)
            output.info("MySQL table name virtual currency fcoin market depth: %s ", self._mysql_table_name_virtual_currency_fcoin_market_depth)
            output.info("MySQL table name virtual currency 58coin kline: %s ", self._mysql_table_name_virtual_currency_fiveeightcoin_kline)
            output.info("MySQL table name virtual currency 58coin market trade: %s ", self._mysql_table_name_virtual_currency_fiveeightcoin_market_trade)
            output.info("MySQL table name virtual currency 58coin market depth: %s ", self._mysql_table_name_virtual_currency_fiveeightcoin_market_depth)
            output.info("MySQL procedure insert legal tender data: %s ", self._mysql_procedure_insert_legal_tender_data)
            output.info("MySQL procedure insert bittrex market depth: %s ", self._mysql_procedure_insert_bittrex_market_depth)
            output.info("MySQL procedure insert bittrex market trade: %s ", self._mysql_procedure_insert_bittrex_market_trade)
            output.info("MySQL procedure insert bittrex kline: %s ", self._mysql_procedure_insert_bittrex_kline)
            output.info("MySQL procedure insert huobi market depth: %s ", self._mysql_procedure_insert_huobi_market_depth)
            output.info("MySQL procedure insert huobi market trade: %s ", self._mysql_procedure_insert_huobi_market_trade)
            output.info("MySQL procedure insert huobi kline: %s ", self._mysql_procedure_insert_huobi_kline)
            output.info("MySQL procedure insert binance market depth: %s ", self._mysql_procedure_insert_binance_market_depth)
            output.info("MySQL procedure insert binance market trade: %s ", self._mysql_procedure_insert_binance_market_trade)
            output.info("MySQL procedure insert binance kline: %s " , self._mysql_procedure_insert_binance_kline)
            output.info("MySQL procedure insert fcoin market depth: %s ", self._mysql_procedure_insert_fcoin_market_depth)
            output.info("MySQL procedure insert fcoin market trade: %s ", self._mysql_procedure_insert_fcoin_market_trade)
            output.info("MySQL procedure insert fcoin kline: %s ", self._mysql_procedure_insert_fcoin_kline)
            output.info("MySQL procedure insert 58coin market depth: %s ", self._mysql_procedure_insert_fiveeightcoin_market_depth)
            output.info("MySQL procedure insert 58coin market trade: %s ", self._mysql_procedure_insert_fiveeightcoin_market_trade)
            output.info("MySQL procedure insert 58coin kline: %s ", self._mysql_procedure_insert_fiveeightcoin_kline)
            output.info("MySQL procedure get_max time market trade: %s ", self._mysql_procedure_get_max_time_market_trade)
            output.info("MySQL procedure get_max time kline: %s ", self._mysql_procedure_get_max_time_kline)
            output.info("Crawl mode: %d ", self._crawl_mode)
            output.info("Crawl max page: %d ", self._crawl_max_page)
            output.info("Sell interval multiple threshold: %.3f ", self._sell_interval_multiple_threshold)
            output.info("Buy interval multiple threshold: %.3f ", self._buy_interval_multiple_threshold)
            output.info("Mail SMTP domain: %s ", self._mail_smtp_domain)
            output.info("Mail SMTP port: %s ", self.__mail_smtp_port)
            output.info("Mail login user: %s ", self._mail_login_user)
            output.info("Mail login pwd: %s ", self._mail_login_pwd)
            output.info("Mail address: %s ", str(self._mail_address))
            output.info("Log file: %s ", self.log_file)
            output.info("Log root dir: %s ", self.log_root_dir)
            output.info("Summary period: %s ", self.summary_period)
            output.info("Low disk space alert: %s ", self.low_disk_space_alert)
            output.info("Low disk space terminal: %s ", self.low_disk_space_terminal)
            output.info("====== Configurations ======")
        else:
            print("====== Configurations ======")
            for key in self.configs:
                print("[%s]" % key)
                for item in self.configs[key]:
                    print("    %s: %s" % (item[0], item[1]))
            print("---------------------------")
            print("Proxy: %s " % self.proxy)
            print("MySQL ip: %s " % self._mysql_ip)
            print("MySQL port: %d " % self._mysql_port)
            print("MySQL user: %s " % self._mysql_user)
            print("MySQL pwd: %s " % self._mysql_pwd)
            print("MySQL database name legal tender huobi: %s " % self._mysql_database_name_legal_tender_huobi)
            print("MySQL table name legal tender huobi: %s " % self._mysql_table_name_legal_tender_huobi)
            print("MySQL table name legal tender total page statistics: %s " % self._mysql_table_name_legal_tender_total_page_statistics)
            print("MySQL database name virtual currency: %s " % self._mysql_database_name_virtual_currency)
            print("MySQL table name virtual currency huobi kline: %s " % self._mysql_table_name_virtual_currency_huobi_kline)
            print("MySQL table name virtual currency huobi market trade: %s " % self._mysql_table_name_virtual_currency_huobi_market_trade)
            print("MySQL table name virtual currency huobi market depth: %s " % self._mysql_table_name_virtual_currency_huobi_market_depth)
            print("MySQL table name virtual currency bittrex kline: %s " % self._mysql_table_name_virtual_currency_bittrex_kline)
            print("MySQL table name virtual currency bittrex market trade: %s " % self._mysql_table_name_virtual_currency_bittrex_market_trade)
            print("MySQL table name virtual currency bittrex market depth: %s " % self._mysql_table_name_virtual_currency_bittrex_market_depth)
            print("MySQL table name virtual currency binance kline: %s " % self._mysql_table_name_virtual_currency_binance_kline)
            print("MySQL table name virtual currency binance market trade: %s " % self._mysql_table_name_virtual_currency_binance_market_trade)
            print("MySQL table name virtual currency binance market depth: %s " % self._mysql_table_name_virtual_currency_binance_market_depth)
            print("MySQL table name virtual currency fcoin kline: %s " % self._mysql_table_name_virtual_currency_fcoin_kline)
            print("MySQL table name virtual currency fcoin market trade: %s " % self._mysql_table_name_virtual_currency_fcoin_market_trade)
            print("MySQL table name virtual currency fcoin market depth: %s " % self._mysql_table_name_virtual_currency_fcoin_market_depth)
            print("MySQL table name virtual currency 58coin kline: %s " % self._mysql_table_name_virtual_currency_fiveeightcoin_kline)
            print("MySQL table name virtual currency 58coin market trade: %s " % self._mysql_table_name_virtual_currency_fiveeightcoin_market_trade)
            print("MySQL table name virtual currency 58coin market depth: %s " % self._mysql_table_name_virtual_currency_fiveeightcoin_market_depth)
            print("MySQL procedure insert legal tender data: %s " % self._mysql_procedure_insert_bittrex_market_depth)
            print("MySQL procedure insert bittrex market depth: %s " % self._mysql_procedure_insert_bittrex_market_depth)
            print("MySQL procedure insert bittrex market trade: %s " % self._mysql_procedure_insert_bittrex_market_trade)
            print("MySQL procedure insert bittrex kline: %s " % self._mysql_procedure_insert_bittrex_kline)
            print("MySQL procedure insert huobi market depth: %s " % self._mysql_procedure_insert_huobi_market_depth)
            print("MySQL procedure insert huobi market trade: %s " % self._mysql_procedure_insert_huobi_market_trade)
            print("MySQL procedure insert huobi kline: %s " % self._mysql_procedure_insert_huobi_kline)
            print("MySQL procedure insert binance market depth: %s " % self._mysql_procedure_insert_binance_market_depth)
            print("MySQL procedure insert binance market trade: %s " % self._mysql_procedure_insert_binance_market_trade)
            print("MySQL procedure insert binance kline: %s " % self._mysql_procedure_insert_binance_kline)
            print("MySQL procedure insert fcoin market depth: %s " % self._mysql_procedure_insert_fcoin_market_depth)
            print("MySQL procedure insert fcoin market trade: %s " % self._mysql_procedure_insert_fcoin_market_trade)
            print("MySQL procedure insert fcoin kline: %s " % self._mysql_procedure_insert_fcoin_kline)
            print("MySQL procedure insert 58coin market depth: %s " % self._mysql_procedure_insert_fiveeightcoin_market_depth)
            print("MySQL procedure insert 58coin market trade: %s " % self._mysql_procedure_insert_fiveeightcoin_market_trade)
            print("MySQL procedure insert 58coin kline: %s " % self._mysql_procedure_insert_fiveeightcoin_kline)
            print("MySQL procedure get_max time market trade: %s " % self._mysql_procedure_get_max_time_market_trade)
            print("MySQL procedure get_max time kline: %s " % self._mysql_procedure_get_max_time_kline)
            print("Crawl period: %s " % self._crawl_period)
            print("Crawl mode: %d " % self._crawl_mode)
            print("Crawl max page: %d " % self._crawl_max_page)
            print("Sell interval multiple threshold: %.3f " % self._sell_interval_multiple_threshold)
            print("Buy interval multiple threshold: %.3f " % self._buy_interval_multiple_threshold)
            print("Mail SMTP domain: %s " % self._mail_smtp_domain)
            print("Mail SMTP port: %s " % self.__mail_smtp_port)
            print("Mail login user: %s " % self._mail_login_user)
            print("Mail login pwd: %s " % self._mail_login_pwd)
            print("Mail address: %s " % str(self._mail_address))
            print("Log File: %s " % self.log_file)
            print("Log root dir: %s " % self.log_root_dir)
            print("Summary period: %s " % self.summary_period)
            print("Low disk space alert: %s " % self.low_disk_space_alert)
            print("Low disk space terminal: %s " % self.low_disk_space_terminal)
            print("====== Configurations ======")

    def _set_properties(self):
        all_defined_sections = [ENGINE, CRAWLER]
        for config_key in all_defined_sections:
            for item in self.configs[config_key]:
                if item[0].upper() == PROXY:
                    self._proxy = item[1]
                elif item[0].upper() == LOG_ROOT_DIR:
                    self._log_file_root_dir = item[1]
                elif item[0].upper() == SUMMARY_PERIOD:
                    self._summary_period = float(item[1])
                elif item[0].upper() == LOW_DISK_SPACE_ALERT:
                    self._low_disk_space_alert = int(item[1])
                elif item[0].upper() == LOW_DISK_SPACE_TERMINAL:
                    self._low_disk_space_terminal = int(item[1])
                elif item[0].upper() == MYSQL_IP:
                    self._mysql_ip = item[1]
                elif item[0].upper() == MYSQL_PORT:
                    self._mysql_port = int(item[1])
                elif item[0].upper() == MYSQL_USER:
                    self._mysql_user = item[1]
                elif item[0].upper() == MYSQL_PASSWD:
                    self._mysql_pwd = item[1]
                elif item[0].upper() == MYSQL_DATABASE_NAME_LEGAL_TENDER_HUOBI:
                    self._mysql_database_name_legal_tender_huobi = item[1]
                elif item[0].upper() == MYSQL_TABLE_NAME_LEGAL_TENDER_HUOBI:
                    self._mysql_table_name_legal_tender_huobi = item[1]
                elif item[0].upper() == MYSQL_TABLE_NAME_LEGAL_TENDER_TOTAL_PAGE_STATISTICS:
                    self._mysql_table_name_legal_tender_total_page_statistics = item[1]
                elif item[0].upper() == MYSQL_DATABASE_NAME_VIRTUAL_CURRENCY:
                    self._mysql_database_name_virtual_currency = item[1]
                elif item[0].upper() == MYSQL_TABLE_NAME_VIRTUAL_CURRENCY_HUOBI_KLINE:
                    self._mysql_table_name_virtual_currency_huobi_kline = item[1]
                elif item[0].upper() == MYSQL_TABLE_NAME_VIRTUAL_CURRENCY_HUOBI_MARKET_TRADE:
                    self._mysql_table_name_virtual_currency_huobi_market_trade = item[1]
                elif item[0].upper() == MYSQL_TABLE_NAME_VIRTUAL_CURRENCY_HUOBI_MARKET_DEPTH:
                    self._mysql_table_name_virtual_currency_huobi_market_depth = item[1]
                elif item[0].upper() == MYSQL_TABLE_NAME_VIRTUAL_CURRENCY_BITTREX_KLINE:
                    self._mysql_table_name_virtual_currency_bittrex_kline = item[1]
                elif item[0].upper() == MYSQL_TABLE_NAME_VIRTUAL_CURRENCY_BITTREX_MARKET_TRADE:
                    self._mysql_table_name_virtual_currency_bittrex_market_trade = item[1]
                elif item[0].upper() == MYSQL_TABLE_NAME_VIRTUAL_CURRENCY_BITTREX_MARKET_DEPTH:
                    self._mysql_table_name_virtual_currency_bittrex_market_depth = item[1]



                elif item[0].upper() == MYSQL_TABLE_NAME_VIRTUAL_CURRENCY_BINANCE_KLINE:
                    self._mysql_table_name_virtual_currency_binance_kline = item[1]
                elif item[0].upper() == MYSQL_TABLE_NAME_VIRTUAL_CURRENCY_BINANCE_MARKET_TRADE:
                    self._mysql_table_name_virtual_currency_binance_market_trade = item[1]
                elif item[0].upper() == MYSQL_TABLE_NAME_VIRTUAL_CURRENCY_BINANCE_MARKET_DEPTH:
                    self._mysql_table_name_virtual_currency_binance_market_depth = item[1]

                elif item[0].upper() == MYSQL_TABLE_NAME_VIRTUAL_CURRENCY_FCOIN_KLINE:
                    self._mysql_table_name_virtual_currency_fcoin_kline = item[1]
                elif item[0].upper() == MYSQL_TABLE_NAME_VIRTUAL_CURRENCY_FCOIN_MARKET_TRADE:
                    self._mysql_table_name_virtual_currency_fcoin_market_trade = item[1]
                elif item[0].upper() == MYSQL_TABLE_NAME_VIRTUAL_CURRENCY_FCOIN_MARKET_DEPTH:
                    self._mysql_table_name_virtual_currency_fcoin_market_depth = item[1]


                elif item[0].upper() == MYSQL_TABLE_NAME_VIRTUAL_CURRENCY_FIVEEIGHTCOIN_KLINE:
                    self._mysql_table_name_virtual_currency_fiveeightcoin_kline = item[1]
                elif item[0].upper() == MYSQL_TABLE_NAME_VIRTUAL_CURRENCY_FIVEEIGHTCOIN_MARKET_TRADE:
                    self._mysql_table_name_virtual_currency_fiveeightcoin_market_trade = item[1]
                elif item[0].upper() == MYSQL_TABLE_NAME_VIRTUAL_CURRENCY_FIVEEIGHTCOIN_MARKET_DEPTH:
                    self._mysql_table_name_virtual_currency_fiveeightcoin_market_depth = item[1]



                elif item[0].upper() == MYSQL_PROCEDURE_INSERT_LEGAL_TENDER_DATA:
                    self._mysql_procedure_insert_legal_tender_data = item[1]
                elif item[0].upper() == MYSQL_PROCEDURE_INSERT_BITTREX_MARKET_DEPTH:
                    self._mysql_procedure_insert_bittrex_market_depth = item[1]
                elif item[0].upper() == MYSQL_PROCEDURE_INSERT_BITTREX_MARKET_TRADE:
                    self._mysql_procedure_insert_bittrex_market_trade = item[1]
                elif item[0].upper() == MYSQL_PROCEDURE_INSERT_BITTREX_KLINE:
                    self._mysql_procedure_insert_bittrex_kline = item[1]
                elif item[0].upper() == MYSQL_PROCEDURE_INSERT_HUOBI_MARKET_DEPTH:
                    self._mysql_procedure_insert_huobi_market_depth = item[1]
                elif item[0].upper() == MYSQL_PROCEDURE_INSERT_HUOBI_MARKET_TRADE:
                    self._mysql_procedure_insert_huobi_market_trade = item[1]
                elif item[0].upper() == MYSQL_PROCEDURE_INSERT_HUOBI_KLINE:
                    self._mysql_procedure_insert_huobi_kline = item[1]



                elif item[0].upper() == MYSQL_PROCEDURE_INSERT_BINANCE_MARKET_DEPTH:
                    self._mysql_procedure_insert_binance_market_depth = item[1]
                elif item[0].upper() == MYSQL_PROCEDURE_INSERT_BINANCE_MARKET_TRADE:
                    self._mysql_procedure_insert_binance_market_trade = item[1]
                elif item[0].upper() == MYSQL_PROCEDURE_INSERT_BINANCE_KLINE:
                    self._mysql_procedure_insert_binance_kline = item[1]

                elif item[0].upper() == MYSQL_PROCEDURE_INSERT_FCOIN_MARKET_DEPTH:
                    self._mysql_procedure_insert_fcoin_market_depth = item[1]
                elif item[0].upper() == MYSQL_PROCEDURE_INSERT_FCOIN_MARKET_TRADE:
                    self._mysql_procedure_insert_fcoin_market_trade = item[1]
                elif item[0].upper() == MYSQL_PROCEDURE_INSERT_FCOIN_KLINE:
                    self._mysql_procedure_insert_fcoin_kline = item[1]

                elif item[0].upper() == MYSQL_PROCEDURE_INSERT_FIVEEIGHTCOIN_MARKET_DEPTH:
                    self._mysql_procedure_insert_fiveeightcoin_market_depth = item[1]
                elif item[0].upper() == MYSQL_PROCEDURE_INSERT_FIVEEIGHTCOIN_MARKET_TRADE:
                    self._mysql_procedure_insert_fiveeightcoin_market_trade = item[1]
                elif item[0].upper() == MYSQL_PROCEDURE_INSERT_FIVEEIGHTCOIN_KLINE:
                    self._mysql_procedure_insert_fiveeightcoin_kline = item[1]


                elif item[0].upper() == MYSQL_PROCEDURE_GET_MAX_TIME_MARKET_TRADE:
                    self._mysql_procedure_get_max_time_market_trade = item[1]
                elif item[0].upper() == MYSQL_PROCEDURE_GET_MAX_TIME_KLINE:
                    self._mysql_procedure_get_max_time_kline = item[1]
                elif item[0].upper() == CRAWL_PERIOD:
                    self._crawl_period = int(item[1])
                elif item[0].upper() == SELL_INTERVAL_MULTIPLE_THRESHOLD:
                    self._sell_interval_multiple_threshold = float(item[1])
                elif item[0].upper() == BUY_INTERVAL_MULTIPLE_THRESHOLD:
                    self._buy_interval_multiple_threshold = float(item[1])
                elif item[0].upper() == MAIL_SMTP_DOMAIN:
                    self._mail_smtp_domain = item[1]
                elif item[0].upper() == MAIL_SMTP_PORT:
                    self.__mail_smtp_port = item[1]
                elif item[0].upper() == MAIL_LOGIN_USER:
                    self._mail_login_user = item[1]
                elif item[0].upper() == MAIL_LOGIN_PASSED:
                    self._mail_login_pwd = item[1]
                elif item[0].upper() == MAIL_ADDRESS:
                    self._mail_address = [mail.strip() for mail in item[1].split(",")]
                elif item[0].upper() == CRAWL_MODE:
                    self._crawl_mode = int(item[1])
                elif item[0].upper() == CRAWL_MAX_PAGE:
                    self._crawl_max_page = int(item[1])

    def _init_configs(self):
        config_template = "config_template.ini"
        if not os.path.exists(config_template):
            raise Exception("config_template.ini do not exists!")

        res = dict()
        # read configurations from config_template as template
        config = configparser.ConfigParser()
        config.read(config_template)
        res.fromkeys(config.sections())
        for section in config.sections():
            config_items = list()
            for item in config.items(section):
                config_items.append(item)

            res[section] = config_items

        return res

    def _update_by_file(self, config_file):
        if not os.path.exists(config_file):
            return

        config = configparser.ConfigParser()
        config.read(config_file)

        for section in config.sections():
            if section not in self.configs:
                continue

            section_swap = self.configs[section]
            for item in config.items(section):
                for swap_item in section_swap:
                    if swap_item[0] == item[0]:
                        section_swap.remove(swap_item)
                        section_swap.append(item)

    def _update_by_memory(self, config_args):
        all_defined_sections = [ENGINE, CRAWLER]
        tmp_configs = copy.deepcopy(self.configs)
        for config_key in all_defined_sections:
            for item in tmp_configs[config_key]:
                if item[0].upper() not in (PROXY):
                    continue

                if item[0].upper() == PROXY:
                    update_value = config_args.proxy
                else:
                    # impossible case
                    raise Exception("Can not handle this arg: %s" % item[0])

                if update_value == "":
                    continue

                self.configs[config_key].remove(item)
                self.configs[config_key].append((item[0], update_value))

    @property
    def config_file(self):
        return self._config_file

    @property
    def proxy(self):
        return self._proxy

    @property
    def log_file(self):
        return self._log_file

    @property
    def log_root_dir(self):
        return self._log_file_root_dir

    @property
    def summary_period(self):
        return self._summary_period

    @property
    def low_disk_space_alert(self):
        return self._low_disk_space_alert

    @property
    def low_disk_space_terminal(self):
        return self._low_disk_space_terminal

    @property
    def mysql_ip(self):
        return self._mysql_ip

    @property
    def mysql_port(self):
        return self._mysql_port

    @property
    def mysql_user(self):
        return self._mysql_user

    @property
    def mysql_pwd(self):
        return self._mysql_pwd

    @property
    def mysql_database_name_legal_tender_huobi(self):
        return self._mysql_database_name_legal_tender_huobi

    @property
    def mysql_table_name_legal_tender_huobi(self):
        return self._mysql_table_name_legal_tender_huobi

    @property
    def mysql_table_name_legal_tender_total_page_statistics(self):
        return self._mysql_table_name_legal_tender_total_page_statistics

    @property
    def mysql_database_name_virtual_currency(self):
        return self._mysql_database_name_virtual_currency

    @property
    def mysql_table_name_virtual_currency_huobi_kline(self):
        return self._mysql_table_name_virtual_currency_huobi_kline

    @property
    def mysql_table_name_virtual_currency_huobi_market_trade(self):
        return self._mysql_table_name_virtual_currency_huobi_market_trade

    @property
    def mysql_table_name_virtual_currency_huobi_market_depth(self):
        return self._mysql_table_name_virtual_currency_huobi_market_depth

    @property
    def mysql_table_name_virtual_currency_bittrex_kline(self):
        return self._mysql_table_name_virtual_currency_bittrex_kline

    @property
    def mysql_table_name_virtual_currency_bittrex_market_trade(self):
        return self._mysql_table_name_virtual_currency_bittrex_market_trade

    @property
    def mysql_table_name_virtual_currency_bittrex_market_depth(self):
        return self._mysql_table_name_virtual_currency_bittrex_market_depth

    @property
    def mysql_table_name_virtual_currency_binance_kline(self):
        return self._mysql_table_name_virtual_currency_binance_kline
    @property
    def mysql_table_name_virtual_currency_binance_market_trade(self):
        return self._mysql_table_name_virtual_currency_binance_market_trade
    @property
    def mysql_table_name_virtual_currency_binance_market_depth(self):
        return self._mysql_table_name_virtual_currency_binance_market_depth


    @property
    def mysql_table_name_virtual_currency_fcoin_kline(self):
        return self._mysql_table_name_virtual_currency_fcoin_kline
    @property
    def mysql_table_name_virtual_currency_fcoin_market_trade(self):
        return self._mysql_table_name_virtual_currency_fcoin_market_trade
    @property
    def mysql_table_name_virtual_currency_fcoin_market_depth(self):
        return self._mysql_table_name_virtual_currency_fcoin_market_depth


    @property
    def mysql_table_name_virtual_currency_fiveeightcoin_kline(self):
        return self._mysql_table_name_virtual_currency_fiveeightcoin_kline
    @property
    def mysql_table_name_virtual_currency_fiveeightcoin_market_trade(self):
        return self._mysql_table_name_virtual_currency_fiveeightcoin_market_trade
    @property
    def mysql_table_name_virtual_currency_fiveeightcoin_market_depth(self):
        return self._mysql_table_name_virtual_currency_fiveeightcoin_market_depth


    @property
    def mysql_procedure_insert_legal_tender_data(self):
        return self._mysql_procedure_insert_legal_tender_data

    @property
    def mysql_procedure_insert_bittrex_market_depth(self):
        return self._mysql_procedure_insert_bittrex_market_depth

    @property
    def mysql_procedure_insert_bittrex_market_trade(self):
        return self._mysql_procedure_insert_bittrex_market_trade

    @property
    def mysql_procedure_insert_bittrex_kline(self):
        return self._mysql_procedure_insert_bittrex_kline

    @property
    def mysql_procedure_insert_huobi_market_depth(self):
        return self._mysql_procedure_insert_huobi_market_depth

    @property
    def mysql_procedure_insert_huobi_market_trade(self):
        return self._mysql_procedure_insert_huobi_market_trade

    @property
    def mysql_procedure_insert_huobi_kline(self):
        return self._mysql_procedure_insert_huobi_kline


    @property
    def mysql_procedure_insert_binance_market_depth(self):
        return self._mysql_procedure_insert_binance_market_depth

    @property
    def mysql_procedure_insert_binance_market_trade(self):
        return self._mysql_procedure_insert_binance_market_trade

    @property
    def mysql_procedure_insert_binance_kline(self):
        return self._mysql_procedure_insert_binance_kline


    @property
    def mysql_procedure_insert_fcoin_market_depth(self):
        return self._mysql_procedure_insert_fcoin_market_depth

    @property
    def mysql_procedure_insert_fcoin_market_trade(self):
        return self._mysql_procedure_insert_fcoin_market_trade

    @property
    def mysql_procedure_insert_fcoin_kline(self):
        return self._mysql_procedure_insert_fcoin_kline


    @property
    def mysql_procedure_insert_fiveeightcoin_market_depth(self):
        return self._mysql_procedure_insert_fiveeightcoin_market_depth

    @property
    def mysql_procedure_insert_fiveeightcoin_market_trade(self):
        return self._mysql_procedure_insert_fiveeightcoin_market_trade

    @property
    def mysql_procedure_insert_fiveeightcoin_kline(self):
        return self._mysql_procedure_insert_fiveeightcoin_kline



    @property
    def mysql_procedure_get_max_time_market_trade(self):
        return self._mysql_procedure_get_max_time_market_trade

    @property
    def mysql_procedure_get_max_time_kline(self):
        return self._mysql_procedure_get_max_time_kline

    @property
    def crawl_period(self):
        return self._crawl_period

    @property
    def sell_interval_multiple_threshold(self):
        return self._sell_interval_multiple_threshold

    @property
    def buy_interval_multiple_threshold(self):
        return self._buy_interval_multiple_threshold

    @property
    def mail_smtp_domain(self):
        return self._mail_smtp_domain

    @property
    def mail_smtp_port(self):
        return self.__mail_smtp_port

    @property
    def mail_login_user(self):
        return self._mail_login_user

    @property
    def mail_login_pwd(self):
        return self._mail_login_pwd

    @property
    def mail_address(self):
        return self._mail_address

    @property
    def crawl_mode(self):
        return self._crawl_mode

    @property
    def crawl_max_page(self):
        return self._crawl_max_page


class Configuration:
    def __init__(self):
        pass

    @classmethod
    def get_configuration(cls, config_file="", config_args=None):
        return ConfigurationSingleton(config_file, config_args)
