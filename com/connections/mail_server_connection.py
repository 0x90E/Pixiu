# -*- coding: utf-8 -*-

import smtplib
from com.core.configuration import Configuration
from com.core.logger import Logger


class MailServerConnection:
    def __init__(self):
        self.config = Configuration.get_configuration()
        self.logger = Logger.get_logger()

    def send_mail(self, subject, content):
        try:
            server = smtplib.SMTP(self.config.mail_smtp_domain, self.config.mail_smtp_port)
            self.logger.debug("Establish connection whit SMTP server successfully")
            server.ehlo()
            server.starttls()
            server.login(self.config.mail_login_user, self.config.mail_login_pwd)
            self.logger.debug("Login SMTP server successfully")

            for recipient in self.config.mail_address:
                if '@' not in recipient:
                    self.logger.debug("%s is not a invalid email address", recipient)
                    continue

                mail_body = '\r\n'.join(['To: %s' % recipient,
                                         'From: %s' % self.config.mail_login_user,
                                         'Subject: %s' % subject,
                                         '', content])

                server.sendmail(self.config.mail_login_user, recipient, mail_body)
                self.logger.debug('Sent mail to %s' % recipient)
        except Exception as e:
            self.logger.error('[Exception]Sending mail %s', e)
        finally:
            server.quit()
