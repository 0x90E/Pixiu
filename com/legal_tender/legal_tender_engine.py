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
from com.legal_tender.pixiu_adapter.pixiu_adapter import PixiuAdapter
from com.legal_tender.exchange_adaptee.huobi_adaptee import HuobiAdaptee


def do_crawl(config_file, log_file):
    Configuration.get_configuration(config_file)
    logger = Logger.get_logger(log_file)
    logger.debug("[%s]Log path: %s" % (os.getpid(), Logger.get_log_file()))
    try:
        logger.debug("==== do_crawl ===== %s" % datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        configure_logging(install_root_handler=False)
        huobi_adaptee = HuobiAdaptee()
        pixiu_adapter = PixiuAdapter(huobi_adaptee)
        ExchangesSpider.pixiu_adapter = pixiu_adapter
        process = CrawlerProcess(get_project_settings())
        process.crawl(ExchangesSpider)
        process.start()
        pixiu_adapter.save_trade_data_into_mysql()

    except Exception as e:
        print("Engine get exception: %s" % e)


class LegalTenderEngine:
    def __init__(self):
        self.logger = Logger.get_logger()
        self.config = Configuration.get_configuration()

    def run(self):
        def do_something(sc):
            s.enter(self.config.crawl_period, 1, do_something, (sc,))
            process = mp.Process(target=do_crawl, args=(self.config.config_file, Logger.get_log_file()))
            process.start()
            time.sleep(self.config.crawl_period)
            # Received SIGTERM, shutting down gracefully. Send again to force
            process.terminate()
            # INFO: Closing spider (shutdown)
            process.terminate()
            self.logger.error("Crawler is overtime, terminate is %d" % process.pid)

        try:
            mp.set_start_method('spawn')
            s = sched.scheduler(time.time, time.sleep)
            s.enter(0, 1, do_something, (s,))
            s.run()
        except Exception as e:
            self.logger.error("Engine get exception: %s", e)
            print(e)

        self.logger.debug("Engine done....!!!")

    def main_thread_run(self):
        try:
            mp.set_start_method('spawn')
            do_crawl(self.config.config_file, Logger.get_log_file())
            print("------> [main_thread_run]crawler down!!!")
        except Exception as e:
            self.logger.error("Engine get exception: %s", e)
            print("Engine get exception %s" % e)
