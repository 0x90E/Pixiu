# -*- coding: utf-8 -*-

import scrapy
from com.core.configuration import Configuration


class ExchangesSpider(scrapy.Spider):
    name = "ExchangesSpider"

    pixiu_adapter = None

    def __init__(self, *args, **kw):
        super(ExchangesSpider, self).__init__(*args, **kw)
        self.config = Configuration.get_configuration()
        if ExchangesSpider.pixiu_adapter is None:
            error_msg = "please assign pixiu adapter function"
            self.logger.error(error_msg)
            raise Exception(error_msg)

    def start_requests(self):
        return ExchangesSpider.pixiu_adapter.get_scrapy_start_requests()
