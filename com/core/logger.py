# -*- coding: utf-8 -*-
import time
import os
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime
from com.core.singleton import singleton
from com.core.configuration import Configuration


@singleton
class LoggerSingleton:
    def __init__(self, log_file=""):
        print("[%s]log init only for once!!!!" % os.getpid())
        self.config = Configuration.get_configuration()
        self.logger = logging.getLogger()
        formatter = logging.Formatter('[%(process)d] %(asctime)s [%(module)s] %(levelname)s: %(message)s')

        # Set file log handler
        if log_file == "":
            log_path = os.path.join(self.config.log_root_dir, datetime.now().strftime("%Y%m%d_%H%M%S"))
            if not os.path.exists(log_path):
                os.makedirs(log_path)
            log_file = os.path.join(log_path, self.config.log_file)
        self._log_file = log_file
        rotating_file_handler = RotatingFileHandler(log_file, maxBytes=10485760)
        rotating_file_handler.setFormatter(formatter)

        self.logger.addHandler(rotating_file_handler)
        self.logger.setLevel(logging.DEBUG)

    @property
    def log_file(self):
        return self._log_file


class Logger:
    log_file = ""
    logger_singleton = None

    def __init__(self):
        pass

    @classmethod
    def get_log_file(cls):
        if Logger.log_file == "":
            raise Exception("Pleas initialize the logger first!")
        return Logger.log_file

    @classmethod
    def get_logger(cls, log_file=""):
        if Logger.logger_singleton is None:
            Logger.logger_singleton = LoggerSingleton(log_file)
            Logger.log_file = Logger.logger_singleton.log_file

        return Logger.logger_singleton.logger
