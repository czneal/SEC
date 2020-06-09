import unittest
import datetime as dt

from functools import partial

import mailer.mailcontents as mc

from mysqlio.tests.dbtest import DBTestBase
from utils import make_absolute


absfilename = partial(make_absolute, file_loc=__file__)


class TestMailerList(DBTestBase):
    def test_read_metadata(self):
        self.run_mysql_file(absfilename('res/mailer_list.sql'))

        mailer = mc.MailerList()
        mailer.read_metadata()

        self.assertEqual(len(mailer.subscribers), 2)
        self.assertEqual(len(mailer.subscriptions), 5)

        for sub, req in mailer.subscribers['vkugushev@gmail.com']:
            if isinstance(req, mc.LogRequest):
                self.assertEqual(req.types, set(
                    ['fatal', 'xbrl', 'stocks', 'shares']))
            if isinstance(req, mc.StocksRequest):
                self.assertEqual(req.data, {
                    "bac": [30, 32],
                    "wfc": [28, 30]})
            if isinstance(req, mc.SharesRequest):
                self.assertEqual(req.tickers, set(
                    ["wfc", "aapl", "brk.a", "ge", "bac"]))

    def test_read_data(self):
        self.run_mysql_file(absfilename('res/mailer_list.sql'))
        self.run_mysql_file(absfilename('res/logs_parse.sql'))
        self.run_mysql_file(absfilename('res/xbrl_logs.sql'))
        self.run_mysql_file(absfilename('res/stocks_shares.sql'))
        self.run_mysql_file(absfilename('res/stocks_dividents.sql'))
        self.run_mysql_file(absfilename('res/sec_xbrl_forms.sql'))

        mailer = mc.MailerList()
        mailer.read_metadata()

        day = dt.date(2020, 6, 1)
        with self.subTest(day=str(day)):
            mailer.read_data(day=day)
            self.assertEqual(len(mailer.subscriptions[4].data_bank.data), 4)
            self.assertTrue(
                len(mailer.subscriptions[4].data_bank.data['fatal']) != 0)
            self.assertEqual(len(mailer.subscriptions[0].data_bank.data), 0)
            self.assertEqual(len(mailer.subscriptions[1].data_bank.data), 0)
            self.assertEqual(len(mailer.subscriptions[2].data_bank.data), 0)
            self.assertEqual(len(mailer.subscriptions[3].data_bank.data), 0)

        day = dt.date(2020, 6, 3)
        with self.subTest(day=str(day)):
            mailer.read_data(day=day)
            self.assertEqual(len(mailer.subscriptions[4].data_bank.data), 4)
            self.assertEqual(
                len(mailer.subscriptions[4].data_bank.data['fatal']), 0)
            self.assertEqual(len(mailer.subscriptions[0].data_bank.data), 0)
            self.assertEqual(len(mailer.subscriptions[1].data_bank.data), 0)
            self.assertEqual(len(mailer.subscriptions[2].data_bank.data), 0)
            self.assertEqual(len(mailer.subscriptions[3].data_bank.data), 1)

    def test_get_messages(self):
        """check if mailer list works"""

        self.run_mysql_file(absfilename('res/mailer_list.sql'))

        mailer = mc.MailerList()
        mailer.read_metadata()

        day = dt.date(2020, 6, 1)
        with self.subTest(day=str(day)):
            mailer.read_data(day=day)
            for subscriber, msg in mailer.get_messages():
                pass

        day = dt.date(2020, 6, 3)
        with self.subTest(day=str(day)):
            mailer.read_data(day=day)
            for subscriber, msg in mailer.get_messages():
                pass


if __name__ == '__main__':
    unittest.main()
