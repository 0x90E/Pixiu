# -*- coding: utf-8 -*-'
from com.core.configuration import Configuration
from com.core.logger import Logger


PIXIU_FIELD_COIN_NAME = "coin_name"
PIXIU_FIELD_USER_ID = "user_id"
PIXIU_FIELD_ACTION = "action"
PIXIU_FIELD_PRICE = "price"
PIXIU_FIELD_COUNT = "amount"
PIXIU_FIELD_PAY_METHOD = "pay_method"
PIXIU_FIELD_IS_FIXED = "is_fixed"
PIXIU_FIELD_MIN_TRADE_LIMIT = "min_trade_limit"
PIXIU_FIELD_MAX_TRADE_LIMIT = "max_trade_limit"
PIXIU_FIELD_CRAWLER_BATCH = "crawler_batch"
PIXIU_FIELD_CRAWLER_TIME = "crawler_time"
PIXIU_FIELD_PAGE = "page"

PIXIU_COIN_NAME_BTC = "btc"
PIXIU_COIN_NAME_USDT = "usdt"
PIXIU_COIN_NAME_ETH = "eth"

PIXIU_ACTION_BUY = "buy"
PIXIU_ACTION_SELL = "sell"

PIXIU_PAY_METHOD_BANK_CARD = "1"
PIXIU_PAY_METHOD_ALIPAY = "2"
PIXIU_PAY_METHOD_WXPAY = "3"
PIXIU_PAY_METHOD_PAYPAL = "4"
PIXIU_PAY_METHOD_WESTERN_UNION = "5"
PIXIU_PAY_METHOD_PAYNOW = "7"
PIXIU_PAY_METHOD_INTERAC_E_TRANSFER = "10" 


class PixiuTarget:
    def __init__(self):
        self.logger = Logger.get_logger()
        self.config = Configuration.get_configuration()

    @classmethod
    def get_pixiu_trade_field(cls):
        return [PIXIU_FIELD_COIN_NAME, PIXIU_FIELD_ACTION, PIXIU_FIELD_PRICE, PIXIU_FIELD_COUNT, PIXIU_FIELD_PAY_METHOD,
                PIXIU_FIELD_IS_FIXED, PIXIU_FIELD_MIN_TRADE_LIMIT, PIXIU_FIELD_MAX_TRADE_LIMIT, PIXIU_FIELD_USER_ID]

    @classmethod
    def get_pixiu_coin_names(cls):
        return [PIXIU_COIN_NAME_BTC, PIXIU_COIN_NAME_USDT, PIXIU_COIN_NAME_ETH]

    @classmethod
    def get_pixiu_actions(cls):
        return [PIXIU_ACTION_BUY, PIXIU_ACTION_SELL]
