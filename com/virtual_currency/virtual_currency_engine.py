# -*- coding: utf-8 -*-
import os
import sched
import time
import datetime
import multiprocessing as mp
from scrapy.crawler import CrawlerProcess
from scrapy.utils.log import configure_logging
from scrapy.utils.project import get_project_settings
from com.core.logger import Logger
from com.core.configuration import Configuration
from exchanges_price_spider.spiders.exchanges_spider import ExchangesSpider
from com.virtual_currency.exchange_adaptee.huobi_adaptee import HuobiAdaptee
from com.virtual_currency.exchange_adaptee.bittrex_adaptee import BittrexAdaptee
from com.virtual_currency.pixiu_adapter.pixiu_adapter import PixiuAdapter
from com.connections.mysql_connection import MysqlConnection, SQL_TIME_FORMAT_WITH_MILLISECOND, SQL_TIME_FORMAT_WITHOUT_SECOND, \
    SQL_TIME_DEFAULT_TIME_WITH_MILLISECOND, SQL_TIME_DEFAULT_TIME_WITHOUT_SECOND
from com.data_collection.market_trade_data_collection import MarketTradeDataCollection
from com.data_collection.kline_data_collection import KlineDataCollection


KLINE_TABLE_NAME = "kline_table_name"
TRADE_TABLE_NAME = "trade_table_name"


def do_crawl_exchange(config_file, log_file, exchange_adaptee, kline_data_collection, market_depth_collection,
                      market_trade_collection, table_name_dict):
    Configuration.get_configuration(config_file)
    logger = Logger.get_logger(log_file)
    logger.debug("[%s]Log path: %s" % (os.getpid(), Logger.get_log_file()))
    try:
        logger.debug("==== do_crawl ===== %s" % datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        configure_logging(install_root_handler=False)
        huobi_adaptee = exchange_adaptee()
        pixiu_adapter = PixiuAdapter(huobi_adaptee)
        ExchangesSpider.pixiu_adapter = pixiu_adapter

        process = CrawlerProcess(get_project_settings())
        process.crawl(ExchangesSpider)
        process.start()

        for item in pixiu_adapter.kline_data_collection:
            kline_data_collection.append(item)

        for item in pixiu_adapter.market_depth_collection:
            market_depth_collection.append(item)

        for item in pixiu_adapter.market_trade_collection:
            market_trade_collection.append(item)

        table_name_dict[KLINE_TABLE_NAME] = pixiu_adapter.kline_table_name
        table_name_dict[TRADE_TABLE_NAME] = pixiu_adapter.trade_table_name

    except Exception as e:
        print("Engine get exception: %s" % e)


class VirtualCurrencyEngine:
    def __init__(self, exchange_adaptee):
        self.logger = Logger.get_logger()
        self.config = Configuration.get_configuration()
        self.mysql_connection = MysqlConnection.get_mysql_connection()
        self._create_and_use_database_connection()
        self._exchange_adaptee = exchange_adaptee

    def run(self):
        def do_something(sc):
            s.enter(self.config.crawl_period, 1, do_something, (sc,))
            mp_manager = mp.Manager()
            kline_data_collection = mp_manager.list()
            market_depth_collection = mp_manager.list()
            market_trade_collection = mp_manager.list()
            table_name_dict = mp_manager.dict()
            # exchange_adaptee = BittrexAdaptee
            exchange_adaptee = self._exchange_adaptee
            process = mp.Process(target=do_crawl_exchange, args=(self.config.config_file, Logger.get_log_file(),
                                                                 exchange_adaptee, kline_data_collection,
                                                                market_depth_collection, market_trade_collection,
                                                                table_name_dict, ))
            self.logger.info("[run] Start process: %s",
                             datetime.datetime.now().strftime(SQL_TIME_FORMAT_WITH_MILLISECOND))
            process.start()
            process.join()
            self.logger.info("[run] End process: %s",
                             datetime.datetime.now().strftime(SQL_TIME_FORMAT_WITH_MILLISECOND))
            kline_table_name = table_name_dict[KLINE_TABLE_NAME]
            trade_table_name = table_name_dict[TRADE_TABLE_NAME]

            max_time_kline = self._get_max_time_kline(kline_table_name)

            filtered_kline_data_collection = KlineDataCollection.filter_kline_data(kline_data_collection, max_time_kline)

            kline_procedure_name = self._exchange_adaptee.get_kline_insert_procedure()
            self._save_data_into_database_by_procedure(kline_procedure_name, filtered_kline_data_collection)

            max_time_market_trade = self._get_max_time_market_trade(trade_table_name)
            filtered_market_trade_data_collection = MarketTradeDataCollection.filter_market_trade_data(
                market_trade_collection, max_time_market_trade)
            market_trade_procedure_name = self._exchange_adaptee.get_market_trade_insert_procedure()
            self._save_data_into_database_by_procedure(market_trade_procedure_name,
                                                       filtered_market_trade_data_collection)

            market_depth_procedure_name = self._exchange_adaptee.get_market_depth_insert_procedure()
            self._save_data_into_database_by_procedure(market_depth_procedure_name,
                                                       market_depth_collection)

        try:
            mp.set_start_method('spawn')
            s = sched.scheduler(time.time, time.sleep)
            s.enter(0, 1, do_something, (s,))
            s.run()
        except Exception as e:
            self.logger.error("Engine get exception: %s", e)
            print(e)

        self.logger.debug("Engine done....!!!")

    def _save_data_into_database_by_procedure(self, procedure_name, collection_data):
        if not self._check_and_rebuild_mysql_connection():
            return False
        self.mysql_connection.insert_trade_data_by_procedure(procedure_name, collection_data)

    def _get_max_time_kline(self, kline_table_name):
        if not self._check_and_rebuild_mysql_connection():
            return False

        procedure_name = self.config.mysql_procedure_get_max_time_kline
        max_time = self.mysql_connection.get_max_time_by_procedure(procedure_name, kline_table_name,
                                                                   SQL_TIME_FORMAT_WITHOUT_SECOND,
                                                                   SQL_TIME_DEFAULT_TIME_WITHOUT_SECOND)
        self.logger.info("get_max_time_kline: %s", max_time)
        return max_time

    def _get_max_time_market_trade(self, market_trade_table_name):
        if not self._check_and_rebuild_mysql_connection():
            return False

        procedure_name = self.config.mysql_procedure_get_max_time_market_trade
        max_time = self.mysql_connection.get_max_time_by_procedure(procedure_name, market_trade_table_name,
                                                                   SQL_TIME_FORMAT_WITH_MILLISECOND,
                                                                   SQL_TIME_DEFAULT_TIME_WITH_MILLISECOND)
        self.logger.info("get_max_time_market_trade: %s", max_time)

        return max_time

    def _check_and_rebuild_mysql_connection(self):
        if not self.mysql_connection.check_connection_and_cursor():
            self.logger.debug("Need to rebuild mysql connection")
            if not self.mysql_connection.close_mysql_database_connection():
                return False

            if not self._create_and_use_database_connection():
                return False
            self.logger.debug("Rebuild mysql connection successfully")
        return True

    def _create_and_use_database_connection(self):
        if not self.mysql_connection.create_mysql_database_connection():
            return False

        if not self.mysql_connection.use_database(self.config.mysql_database_name_virtual_currency):
            return False

        return True

    def main_thread_run(self):
        try:
            mp.set_start_method('spawn')
            queue = mp.Queue()
            do_crawl_bittrex(queue, self.config.config_file, Logger.get_log_file())
            # pixiu_adapter = queue.get()
            # do_crawl_huobi(self.config.config_file, Logger.get_log_file())
            print("------> [main_thread_run]crawler down!!!")
        except Exception as e:
            self.logger.error("Engine get exception: %s", e)
            print("Engine get exception %s" % e)
