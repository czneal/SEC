import unittest
import datetime as dt

from mysqlio.tests.dbtest import DBTestBase
import mailer.readers as mr


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

        self.assertEqual(len(data), 5)


if __name__ == '__main__':
    unittest.main()
