import unittest
import datetime as dt

from functools import partial
from typing import List

import mailer.mailcontents as mc

from mysqlio.tests.dbtest import DBTestBase
from utils import make_absolute

absfilename = partial(make_absolute, file_loc=__file__)


class TestSubscriptionInfo(DBTestBase):
    def test_reports_info(self):
        self.run_mysql_file(absfilename('res/sec_xbrl_forms.sql'))

        info = mc.ReportsInfo()
        requests: List[mc.ReportsRequest] = [
            mc.ReportsRequest(
                {
                    "1616318": ['y', 'q'],
                    "1125699": ['y']
                }),
            mc.ReportsRequest(
                {
                    "1616318": ["y"],
                    "1376804": ["y"],
                    "1415744": ["y"]
                }
            )]

        info.reset()
        for r in requests:
            info.append_request(r)
        self.assertEqual(len(info.ciks), 4)

        day = dt.date(2020, 6, 3)
        info.read(day=day)
        self.assertEqual(len(info.data), 2)

        inf = info.get_info(requests[0])
        self.assertEqual(len(inf.data), 1)

        inf = info.get_info(requests[1])
        self.assertEqual(len(inf.data), 2)

    def test_dividents_info(self):
        self.run_mysql_file(absfilename('res/stocks_dividents.sql'))

        info = mc.DividentsInfo()
        requests: List[mc.DivRequest] = [
            mc.DivRequest(['aapl', 'amd']),
            mc.DivRequest(['bac', 'wfc', 'amd'])]

        info.reset()
        for r in requests:
            info.append_request(r)
        self.assertEqual(len(info.tickers), 4)

        day = dt.date(2020, 4, 28)
        with self.subTest(day=str(day)):
            info.read(day=day)
            self.assertEqual(len(info.data), 1)
            self.assertTrue('WFC' in info.data)

            inf = info.get_info(requests[0])
            self.assertEqual(len(inf.data), 0)

            inf = info.get_info(requests[1])
            self.assertEqual(len(inf.data), 1)
            self.assertEqual(inf.data[0]['ticker'], 'WFC')

        day = dt.date(2020, 4, 30)
        with self.subTest(day=str(day)):
            info.read(day=day)
            self.assertEqual(len(info.data), 1)
            self.assertTrue('AAPL' in info.data)

            inf = info.get_info(requests[0])
            self.assertEqual(len(inf.data), 1)
            self.assertEqual(inf.data[0]['ticker'], 'AAPL')

            inf = info.get_info(requests[1])
            self.assertEqual(len(inf.data), 0)

        day = dt.date(2020, 7, 1)
        with self.subTest(day=str(day)):
            info.reset()
            req = mc.DivRequest(['TTT', 'AAPL'])
            info.append_request(req)
            self.assertEqual(len(info.tickers), 2)
            self.assertTrue('AAPL' in info.tickers)
            self.assertTrue('TTT' in info.tickers)

            info.read(day=day)
            self.assertEqual(len(info.data), 1)

            inf = info.get_info(req)
            self.assertEqual(len(inf.data), 1)
            self.assertEqual(inf.data[0]['ticker'], 'TTT')
            self.assertTrue(inf.data[0]['amount'] is None)
            self.assertTrue(inf.data[0]['ex_eff_date'] is None)

    def test_shares_info(self):
        self.run_mysql_file(absfilename('res/stocks_shares.sql'))

        info = mc.SharesInfo()
        requests: List[mc.SharesRequest] = [
            mc.SharesRequest(['aapl', 'amd']),
            mc.SharesRequest(['bac', 'wfc', 'amd'])]

        info.reset()
        for r in requests:
            info.append_request(r)
        self.assertEqual(len(info.tickers), 4)

        day = dt.date(2020, 3, 11)
        with self.subTest(day=str(day)):
            info.read(day=day)

            self.assertEqual(len(info.data), 1)

            resp = info.get_info(requests[0])
            self.assertEqual(len(resp.data), 0)

            resp = info.get_info(requests[1])
            self.assertEqual(len(resp.data), 1)
            self.assertEqual(resp.data[0]['ticker'], 'BAC')
            self.assertEqual(resp.data[0]['change'], -3759062)

        day = dt.date(2020, 2, 28)
        with self.subTest(day=str(day)):
            info.read(day=day)

            self.assertEqual(len(info.data), 0)

            resp = info.get_info(requests[0])
            self.assertEqual(len(resp.data), 0)

            resp = info.get_info(requests[1])
            self.assertEqual(len(resp.data), 0)

    def test_stocks_info(self):
        self.run_mysql_file(absfilename('res/stocks_daily.sql'))

        info = mc.StocksInfo()
        requests: List[mc.StocksRequest] = [
            mc.StocksRequest({'aapl': [300, 320],
                              'amd': [10, 20]
                              }),
            mc.StocksRequest({'bac': [35, 40], 'wfc':[40, 60], 'amd':[30, 40]})]

        info.reset()
        for r in requests:
            info.append_request(r)
        self.assertEqual(len(info.tickers), 4)

        day = dt.date(2020, 2, 7)
        with self.subTest(day=str(day)):
            info.read(day=day)

            self.assertEqual(len(info.data), 3)

            resp = info.get_info(requests[0])
            d = [{'close': 320.03, 'more than': 320, 'ticker': 'AAPL'}]
            self.assertEqual(len(resp.data), 1)
            self.assertEqual(resp.data[0]['ticker'], 'AAPL')
            self.assertEqual(resp.data, d)

            resp = info.get_info(requests[1])
            self.assertEqual(len(resp.data), 1)
            d = [{'close': 34.61, 'less than': 35, 'ticker': 'BAC'}]
            self.assertEqual(resp.data[0]['ticker'], 'BAC')
            self.assertEqual(resp.data, d)

        day = dt.date(2020, 2, 5)
        with self.subTest(day=str(day)):
            info.reset()
            requests[1].data['WFC'] = [30, 40]
            info.append_request(requests[0])
            info.append_request(requests[1])

            info.read(day=day)

            self.assertEqual(len(info.data), 3)

            resp = info.get_info(requests[0])
            self.assertEqual(len(resp.data), 1)
            self.assertEqual(resp.data[0]['ticker'], 'AAPL')

            resp = info.get_info(requests[1])
            self.assertEqual(len(resp.data), 2)
            tickers = set([d['ticker'] for d in resp.data])

            self.assertEqual(tickers, {'BAC', 'WFC'})

    def test_log_request(self):
        with self.subTest('assert raise'):
            self.assertRaises(AssertionError, mc.LogRequest, ['fatal', 'xblr'])

        with self.subTest('passes'):
            req = mc.LogRequest(['fatal', 'stocks', 'xbrl'])

    def test_log_info(self):
        self.run_mysql_file(absfilename('res/xbrl_logs.sql'))
        self.run_mysql_file(absfilename('res/logs_parse.sql'))

        info = mc.LogInfo()
        requests = [mc.LogRequest(['fatal', 'xbrl']),
                    mc.LogRequest(['fatal', 'stocks'])]

        for r in requests:
            info.append_request(r)

        self.assertEqual(len(info.types), 3)

        day = dt.date(2020, 6, 1)
        with self.subTest(day=str(day)):
            info.read(day=day)

            self.assertEqual(len(info.data['fatal']), 2)
            self.assertEqual(len(info.data['stocks']), 2)
            self.assertEqual(len(info.data['xbrl']), 1)

            self.assertEqual(
                info.data['xbrl'][0]['extra']['period_dei'],
                '2019-12-31')
            self.assertTrue(
                info.data['fatal'][0]['extra']['msg'].startswith('test'))
            self.assertEqual(
                info.data['stocks'][0]['extra'], '')

            resp = info.get_info(requests[0])
            self.assertEqual(len(resp.data), 3)

            resp = info.get_info(requests[1])
            self.assertEqual(len(resp.data), 4)

        day = dt.date(2020, 6, 2)
        with self.subTest(day=str(day)):
            info.read(day=day)

            self.assertEqual(len(info.data['fatal']), 0)
            self.assertEqual(len(info.data['stocks']), 0)
            self.assertEqual(len(info.data['xbrl']), 0)

            resp = info.get_info(requests[0])
            self.assertEqual(len(resp.data), 0)

            resp = info.get_info(requests[1])
            self.assertEqual(len(resp.data), 0)


if __name__ == '__main__':
    unittest.main()
