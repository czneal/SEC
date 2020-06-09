import unittest
import datetime as dt

import mailer.readers as mr

from mysqlio.tests.dbtest import DBTestBase
from utils import make_absolute
from functools import partial

absfilename = partial(make_absolute, file_loc=__file__)


class TestMailerInfoReader(DBTestBase):
    def test_fetch_stocks(self):
        # setup
        queries = [
            """delete from stocks_daily \
                where ticker in ('aapl', 'wfc', 'bac') \
                   and trade_date = '2020-05-26'
            """,
            """insert into stocks_daily \
               select * from reports.stocks_daily \
               where ticker in ('aapl', 'wfc', 'bac') \
                   and trade_date = '2020-05-26'""",
        ]
        self.run_set_up_queries(queries, [{}, {}])

        # test
        r = mr.MailerInfoReader()

        data = r.fetch_stocks_info(
            day=dt.date(2020, 5, 26),
            tickers=['aapl', 'bac'])
        r.close()

        self.assertEqual(len(data), 2)
        self.assertEqual(data[0]['trade_date'], dt.date(2020, 5, 26))

    def test_fetch_mailer_list(self):
        # setup
        queries = [
            """delete from mailer_list""",
            """insert into mailer_list \
               select * from reports.mailer_list \
               where email in ('vkugushev@gmail.com', 'victor@machinegrading.ee')""",
        ]
        self.run_set_up_queries(queries, [{}, {}])

        # test
        r = mr.MailerInfoReader()
        data = r.fetch_mailer_list()
        r.close()

        self.assertEqual(len(data), 7)

    def test_fecth_reports(self):
        self.run_mysql_file(absfilename('res/sec_xbrl_forms.sql'))

        r = mr.MailerInfoReader()
        day = dt.date(2020, 6, 3)
        response = r.fetch_reports_info(
            day=day,
            ciks=[1066923, 1399306, 880807])
        r.close()

        self.assertEqual(len(response), 1)

        for r in response:
            self.assertEqual(r['stamp'].date(), day)
            self.assertTrue(r['cik'] in (1399306,))

    def test_fetch_dividents(self):
        self.run_mysql_file(absfilename('res/stocks_dividents.sql'))

        r = mr.MailerInfoReader()
        day = dt.date(2020, 4, 30)
        response = r.fetch_dividents_info(
            day=day, tickers=['aapl', 'amd', 'jpm'])

        self.assertTrue(len(response), 1)
        self.assertEqual(response[0]['stamp'].date(), day)
        self.assertEqual(response[0]['ticker'].lower(), 'aapl')


class TestLogReader(DBTestBase):
    def test_fetch_fatal_errors(self):
        self.run_mysql_file(absfilename('res/logs_parse.sql'))

        day = dt.date(2020, 6, 1)
        with self.subTest(day=str(day) + ' every message'):
            r = mr.LogReader()
            data = r.fetch_errors(
                day=day,
                log_table='logs_parse',
                levelname='ERROR',
                msg='')
            r.close()

            self.assertEqual(len(data), 2)
            self.assertEqual(data[0]['created'].date(), day)
            self.assertTrue(data[0]['msg'].startswith('unexpected error'))
            self.assertEqual(data[1]['created'].date(), day)
            self.assertTrue(data[1]['msg'].startswith('unexpected error'))

        day = dt.date(2020, 6, 1)
        with self.subTest(day=str(day) + ' unexpected error 2'):
            r = mr.LogReader()
            data = r.fetch_errors(
                day=day,
                log_table='logs_parse',
                levelname='ERROR',
                msg='unexpected error 2')
            r.close()

            self.assertEqual(len(data), 1)
            self.assertEqual(data[0]['created'].date(), day)
            self.assertTrue(data[0]['msg'].startswith('unexpected error 2'))

        day = dt.date(2020, 6, 3)
        with self.subTest(day=str(day) + ' unexpected error 2'):
            r = mr.LogReader()
            data = r.fetch_errors(
                day=day,
                log_table='logs_parse',
                levelname='ERROR',
                msg='')
            r.close()

            self.assertEqual(len(data), 0)

    def test_fetch_stocks_warning(self):
        self.run_mysql_file(absfilename('res/logs_parse.sql'))

        day = dt.date(2020, 6, 1)
        with self.subTest(day=str(day) + ' denied request'):
            r = mr.LogReader()
            data = r.fetch_errors(
                day=day,
                log_table='logs_parse',
                levelname='warning',
                msg='denied')
            r.close()

            self.assertEqual(len(data), 1)

        day = dt.date(2020, 6, 1)
        with self.subTest(day=str(day) + ' shares doesnt exist'):
            r = mr.LogReader()
            data = r.fetch_errors(
                day=day,
                log_table='logs_parse',
                levelname='warning',
                msg='ticker AAPL shares doesn')
            r.close()

            self.assertEqual(len(data), 1)

    def test_fetch_xbrl_logs(self):
        self.run_mysql_file(absfilename('res/xbrl_logs.sql'))

        day = dt.date(2020, 6, 1)
        with self.subTest(day=str(day) + ' parse errors'):
            r = mr.LogReader()
            data = r.fetch_errors(
                day=day,
                log_table='xbrl_logs',
                levelname='error',
                msg='')
            r.close()

            self.assertEqual(len(data), 1)
            self.assertEqual(data[0]['msg'], 'period match failed')

        day = dt.date(2020, 6, 2)
        with self.subTest(day=str(day) + ' no parse errors'):
            r = mr.LogReader()
            data = r.fetch_errors(
                day=day,
                log_table='xbrl_logs',
                levelname='error',
                msg='')
            r.close()

            self.assertEqual(len(data), 0)

    def test_fetch_share_ticker_warning(self):
        self.run_mysql_file(absfilename('res/xbrl_logs.sql'))

        day = dt.date(2020, 5, 30)
        with self.subTest(day=str(day) + ' share-ticker relation'):
            r = mr.LogReader()
            data = r.fetch_errors(
                day=day,
                log_table='xbrl_logs',
                levelname='warning',
                msg='share-ticker')
            r.close()

            self.assertEqual(len(data), 3)
            self.assertEqual(data[0]['msg'],
                             'sec share-ticker relaition not found')

        day = dt.date(2020, 6, 1)
        with self.subTest(day=str(day) + ' share-ticker relation'):
            r = mr.LogReader()
            data = r.fetch_errors(
                day=day,
                log_table='xbrl_logs',
                levelname='warning',
                msg='share-ticker')
            r.close()

            self.assertEqual(len(data), 0)


if __name__ == '__main__':
    unittest.main()
