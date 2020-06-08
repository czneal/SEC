import datetime as dt

import logs

from mailer.sendmail import send_message
from mailer.mailcontents import MailerList


def send_mails(day: dt.date):
    logger = logs.get_logger(__name__)
    try:
        logger.info('start sending mails')
        mailer = MailerList()
        mailer.read_metadata()

        logger.debug(f'read data for {day}')
        mailer.read_data(day=day)

        for to, msg in mailer.get_messages():
            logger.info(f'send mail to: {to}')
            send_message(to=to,
                         subject='Daily Info',
                         content=msg)
        logger.info('finish sending mails')
    except Exception:
        logger.error('unexpected error', exc_info=True)


if __name__ == '__main__':
    send_mails(dt.date(2020, 2, 4))
