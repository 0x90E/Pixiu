# -*- coding: utf-8 -*-
import datetime
import MySQLdb
from com.core.singleton import singleton
from com.core.configuration import Configuration
from com.core.logger import Logger

SQL_TIME_FORMAT = "%Y-%m-%d %H:%M:%S"
SQL_TIME_FORMAT_WITHOUT_SECOND = "%Y-%m-%d %H:%M"
SQL_TIME_FORMAT_WITH_MILLISECOND = "%Y-%m-%d %H:%M:%S,%f"
SQL_TIME_FORMAT_MILLISECOND_DELIMITER = ","


SQL_TIME_DEFAULT_TIME_WITHOUT_SECOND = "1970-01-01 00:00"
SQL_TIME_DEFAULT_TIME = "1970-01-01 00:00:00"
SQL_TIME_DEFAULT_TIME_WITH_MILLISECOND = "1970-01-01 00:00:00,000"


@singleton
class MysqlConnectionSingleton:
    def __init__(self):
        self.config = Configuration.get_configuration()
        self.config.show()
        self.logger = Logger.get_logger()
        self._mysql_conn = None
        self._cursor = None

    def create_mysql_database_connection(self):
        try:
            if self._mysql_conn is not None:
                self.logger.debug("MySql connection is not None")
                return False

            if self._cursor is not None:
                self.logger.debug("Cursor is not None")
                return False

            self._mysql_conn = MySQLdb.connect(host=self.config.mysql_ip, port=self.config.mysql_port,
                                               user=self.config.mysql_user, passwd=self.config.mysql_pwd,
                                               charset="utf8")
            self._cursor = self._mysql_conn.cursor()

            return True
        except MySQLdb.OperationalError as e:
            self.logger.error("Create mysql database %s", e)

        return False

    def close_mysql_database_connection(self):
        try:
            if self._cursor is not None:
                self._cursor.close()
                self._cursor = None
            if self._mysql_conn is not None:
                self._mysql_conn.close()
                self._mysql_conn = None

            return True
        except MySQLdb.OperationalError as e:
            self.logger.error("Close mysql database %s", e)

        return False

    def create_table(self, create_table, create_sql_statement):
        if not self.check_connection_and_cursor():
            return
        self.use_database(create_table)
        self._cursor.execute(create_sql_statement)

    def create_and_database(self, database_name):
        if not self.check_connection_and_cursor():
            return
        sql_create_database_if_not_exists = "CREATE DATABASE IF NOT EXISTS " + database_name + " ;"
        self._cursor.execute(sql_create_database_if_not_exists)

    def get_extremum_of_the_last_period(self, this_period, coin_name, action, extremum_fun):
        query = ("SELECT crawl_time, coin_name, action, price, amount FROM huobi_price_monitor "
                 "WHERE crawl_time BETWEEN %s AND %s AND coin_name = %s AND action = %s")

        this_period_datetime = datetime.datetime.strptime(this_period, SQL_TIME_FORMAT)
        the_last_period = \
            (this_period_datetime - datetime.timedelta(0, self.config.crawl_period)).strftime(SQL_TIME_FORMAT)
        # avoid get date of this period
        self.logger.info("[Before]this_period: %s " % this_period)
        this_period = (this_period_datetime - datetime.timedelta(0, 1)).strftime(SQL_TIME_FORMAT)
        self.logger.info("[After]this_period: %s " % this_period)
        self.logger.info("the_last_period: %s " % the_last_period)

        self._cursor.execute(query, (the_last_period, this_period, coin_name, action))

        extremum_value = None
        # maybe have no result data
        for (crawl_time, coin_name, action, price, amount) in self._cursor:
            print("{} {} {} {} {}".format(crawl_time, coin_name, action, price, amount))
            extremum_fun(price)

    def insert_trade_data_by_procedure(self, procedure_name, insert_trade_info_sql_statement_list):
        try:
            self.logger.info("Procedure Name: %s Length of info: %d",
                             procedure_name, len(insert_trade_info_sql_statement_list))
            if len(insert_trade_info_sql_statement_list) == 0:
                return

            for trade_info_sql_statement in insert_trade_info_sql_statement_list:
                self._cursor.callproc(procedure_name, trade_info_sql_statement)
                self._cursor.fetchall()
                self._cursor.close()
                self._cursor = self._mysql_conn.cursor()
                self.logger.info("insert done: %s", trade_info_sql_statement)

            self._mysql_conn.commit()
        except MySQLdb.OperationalError as e:
            self._mysql_conn.rollback()
            self.logger.error("[insert_trade_data_by_procedure]Save data into mysql %s", e)

    def get_max_time_by_procedure(self, procedure_name, table_name, time_format, default_time):
        max_time = default_time
        try:
            self._cursor.callproc(procedure_name, [table_name, ])
            procedure_result = self._cursor.fetchall()
            if len(procedure_result) > 0: max_time = procedure_result[0][0]
            self._cursor.close()
            self._cursor = self._mysql_conn.cursor()
            self._mysql_conn.commit()
        except MySQLdb.OperationalError as e:
            self._mysql_conn.rollback()
            self.logger.error("[get_max_time_by_procedure]Error %s", e)


        datetime_max_time = datetime.datetime.strptime(max_time, time_format)
        return datetime_max_time

    def insert(self, sql_insert):
        try:
            self._cursor.execute(sql_insert)
            self._mysql_conn.commit()
        except MySQLdb.OperationalError as e:
            self._mysql_conn.rollback()
            self.logger.error("[insert]Save data into mysql %s", e)

    def check_connection_and_cursor(self):
        if self._mysql_conn is None:
            self.logger.debug("MySql connection is None, please call create_mysql_database_connection first")
            return False

        if self._cursor is None:
            self.logger.debug("Cursor is None, please call create_mysql_database_connection first")
            return False

        return True

    def use_database(self, database_name):
        sql_use_database = "use " + database_name + " ;"
        self._cursor.execute(sql_use_database)
        return True


class MysqlConnection:
    def __init__(self):
        pass

    @classmethod
    def get_mysql_connection(cls):
        return MysqlConnectionSingleton()
