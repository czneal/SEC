import datetime as dt

from mailer.sendmail import send_message
from mailer.mailcontents import make_parse_info


def main():
    info = make_parse_info(day=dt.date(2020, 5, 21))

    send_message(to='vkugushev@gmail.com',
                 subject='Daily Parse Info',
                 content=info)


if __name__ == '__main__':
    main()
