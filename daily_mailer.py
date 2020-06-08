import datetime as dt
import sys
import argparse

import logs

from mailer.sendmail import send_message
from mailer.glue import send_mails


def init_argparser() -> argparse.ArgumentParser:

    parser = argparse.ArgumentParser(
        usage="%(prog)s date",
        description="Run mailer for specified date"
    )
    parser.add_argument('date', nargs='?')
    return parser


def main() -> None:
    parser = init_argparser()
    args = parser.parse_args()

    if not args.date:
        day = dt.date.today()
    else:
        try:
            parts = args.date.split('-')
            day = dt.date(int(parts[0]),
                          int(parts[1]),
                          int(parts[2]))
        except (IndexError, ValueError):
            print('date must be in format like 2020-01-01')
            return

    logs.configure('mysql', level=logs.logging.INFO)
    send_mails(day=day)


if __name__ == '__main__':
    main()
