#!/usr/bin/env python
# -*- coding: utf-8 -*-
import argparse
import os
from com.core.configuration import Configuration
from com.core.logger import Logger
from com.legal_tender.legal_tender_engine import LegalTenderEngine
from com.virtual_currency.virtual_currency_engine import VirtualCurrencyEngine
from com.virtual_currency.exchange_adaptee.huobi_adaptee import HuobiAdaptee
from com.virtual_currency.exchange_adaptee.bittrex_adaptee import BittrexAdaptee
from com.virtual_currency.exchange_adaptee.binance_adaptee import BianceAdaptee
from com.virtual_currency.exchange_adaptee.fcoin_adaptee import FcoinAdaptee
from com.virtual_currency.exchange_adaptee.fiveeightcoin_adaptee import FiveEightCoinAdaptee


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='''
    Pixiu.
    Pixiu has always been regarded as an auspicious creature that possessed mystical powers capable of drawing Cai Qi 
    from all directions.''',
        formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('-c', metavar='configure', dest='configuration', type=str, action='store',
                        required=True, help='configure file path')
    parser.add_argument('-m', metavar='mode', dest='mode', type=int, action='store',
                        required=True, help="assign the crawler target\n"
                                            "0: crawl legal tender info\n"
                                            "1: crawl virtual currencies info\n")

    args = parser.parse_args()
    if not os.path.exists(args.configuration):
        print("Configure file not exists!")
        exit(1)

    # Do not use the logger before ConfigurationSingleton initialization
    print("args.configuration: %s" % args.configuration)
    print("args.mode: %d" % args.mode)
    print("args: %s" % args)
    config = Configuration.get_configuration(args.configuration, args)

    config.show(Logger.get_logger())

    if args.mode == 0:
        legal_tender_engine = LegalTenderEngine()
        # legal_tender_engine.run()
        legal_tender_engine.main_thread_run()
    elif args.mode == 1:
        virtual_currency_engine = VirtualCurrencyEngine(HuobiAdaptee)
        # virtual_currency_engine.main_thread_run()
        virtual_currency_engine.run()
    elif args.mode == 2:
        virtual_currency_engine = VirtualCurrencyEngine(BittrexAdaptee)
        # virtual_currency_engine.main_thread_run()
        virtual_currency_engine.run()
    elif args.mode == 3:
        virtual_currency_engine = VirtualCurrencyEngine(BianceAdaptee)
        # virtual_currency_engine.main_thread_run()
        virtual_currency_engine.run()
    elif args.mode == 4:
        virtual_currency_engine = VirtualCurrencyEngine(FcoinAdaptee)
        # virtual_currency_engine.main_thread_run()
        virtual_currency_engine.run()
    elif args.mode == 5:
        virtual_currency_engine = VirtualCurrencyEngine(FiveEightCoinAdaptee)
        # virtual_currency_engine.main_thread_run()
        virtual_currency_engine.run()
    else:
        raise Exception("Unknown mode")

