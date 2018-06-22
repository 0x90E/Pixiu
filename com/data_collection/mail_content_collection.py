# -*- coding: utf-8 -*-

from com.core.configuration import Configuration
from com.core.logger import Logger


class MailContentCollection:
    def __init__(self):
        self.config = Configuration.get_configuration()
        self.logger = Logger.get_logger()
        self._content = ""
        self._trade_deficit = ""
        self.__add_content_header()
        self._add_arbitrage_part_header()
        self.arbitrage_mail_subject = ""

    def get_arbitrage_mail_subject(self):
        mail_subject = self.arbitrage_mail_subject
        subject = "{}".format(mail_subject)
        self.logger.info("subject: %s", subject)
        return subject

    def get_mail_content(self):
        self._add_arbitrage_part_footer()
        self._add_content(self._trade_deficit)

        self.__add_content_footer()
        self.logger.info("\n" + self._content)
        return self._content

    def add_arbitrage_part(self, coin_name, buy_price, sell_price, buy_amount, sell_amount, buy_min_trade_limit,
                           user_web_url):
        arbitrage_msg = "[{}]Buy: {:.2f}({}), Sell {:.2f}({}), Link {}\n".format(coin_name, buy_price, buy_amount,
                                                                                  sell_price, sell_amount, user_web_url)
        self._trade_deficit = self._trade_deficit + arbitrage_msg
        if self.arbitrage_mail_subject == "":
            self.arbitrage_mail_subject = "[{}]Buy: {:.2f}({})(CNY:{}), Sell {:.2f}({})".format(coin_name, buy_price, buy_amount,
                                                                                   buy_min_trade_limit, sell_price, sell_amount)

    def _add_arbitrage_part_header(self):
        self._trade_deficit = "===== Trbitrage =====\n"

    def _add_arbitrage_part_footer(self):
        self._trade_deficit += "===== Trbitrage =====\n"

    def _add_content(self, msg):
        self._content += msg

    def __add_content_header(self):
        self._content = "======== Remind mail ========\n"

    def __add_content_footer(self):
        self._content += "======== Remind mail ========\n"
