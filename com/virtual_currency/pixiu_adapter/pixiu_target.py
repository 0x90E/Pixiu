# -*- coding: utf-8 -*-
from com.core.logger import Logger
from com.core.configuration import Configuration

PIXIU_KLINE_DATA = "date"
PIXIU_KLINE_AMOUNT = "amount"
PIXIU_KLINE_OPEN_PRICE = "open_price"
PIXIU_KLINE_CLOSE_PRICE = "close_price"
PIXIU_KLINE_LOW_PRICE = "low_price"
PIXIU_KLINE_HIGH_PRICE = "high_price"
PIXIU_KLINE_TURNOVER = "turnover"

PIXIU_MARKET_TRADE_DATA = "date"
PIXIU_MARKET_TRADE_PRICE = "price"
PIXIU_MARKET_TRADE_AMOUNT = "amount"
PIXIU_MARKET_TRADE_DIRECTION =  "direction"

PIXIU_MARKET_TRADE_DIRECTION_BUY = "buy"
PIXIU_MARKET_TRADE_DIRECTION_SELL= "sell"

PIXIU_MARKET_DEPTH_DATA = "date"
PIXIU_MARKET_DEPTH_RESPONSE_TIME = "response_time"
PIXIU_MARKET_DEPTH_PRICE = "price"
PIXIU_MARKET_DEPTH_AMOUNT = "amount"
PIXIU_MARKET_DEPTH_TYPE = "type"

PIXIU_MARKET_DEPTH_TYPE_BID = "bid"
PIXIU_MARKET_DEPTH_TYPE_ASK = "ask"


class PixiuTarget:
    def __init__(self):
        self.logger = Logger.get_logger()
        self.config = Configuration.get_configuration()

    def get_scrapy_start_requests(self):
        print("PixiuTarget get_scrapy_start_requests")
        # pass

