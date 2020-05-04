import unittest
import unittest.mock
import json
import datetime as dt
import numpy as np
import pandas as pd
import os

import firms.tickers as t
from utils import make_absolute


class TestStockData(unittest.TestCase):
    def test_access_denied(self):
        with unittest.mock.patch('firms.tickers.fetch_with_delay') as fetch,\
                unittest.mock.patch('logs.get_logger') as get_logger:

            fetch.return_value = b'<html><title>Access Denied</title></html>'
            logger = unittest.mock.MagicMock()
            get_logger.return_value = logger

            data = t.stock_data('AAPL')

            self.assertEqual(data["trade_date"], None)
            logger.warning.assert_has_calls([unittest.mock.call(
                msg='nasdaq site denied request for tikcer AAPL')])

    def test_stock_data_info_summary(self):
        test_data = {'pre hours': ["""{"data":{"symbol":"AAPL","companyName":"Apple Inc. Common Stock","stockType":"Common Stock","exchange":"NASDAQ-GS","isNasdaqListed":true,"isNasdaq100":true,"isHeld":false,"primaryData":{"lastSalePrice":"$267.85","netChange":"0.75","percentageChange":"0.28%","deltaIndicator":"up","lastTradeTimestamp":"DATA AS OF Nov 19, 2019 9:27 AM ET - PRE-MARKET","isRealTime":true},"secondaryData":{"lastSalePrice":"$267.10","netChange":"1.34","percentageChange":"0.5%","deltaIndicator":"up","lastTradeTimestamp":"CLOSED AT 4:00 PM ET ON Nov 18, 2019","isRealTime":false},"keyStats":{"Volume":{"label":"Volume","value":"111,677"},"PreviousClose":{"label":"Previous Close","value":"$267.10"},"OpenPrice":{"label":"Open","value":"$265.74"},"MarketCap":{"label":"Market Cap","value":"1,186,796,081,500"}},"marketStatus":"Pre Market","assetClass":"STOCKS"},"message":null,"status":{"rCode":200,"bCodeMessage":null,"developerMessage":null}}""",
                                   """{"data":{"symbol":"AAPL","summaryData":{"Exchange":{"label":"Exchange","value":"NASDAQ-GS"},"Sector":{"label":"Sector","value":"Technology"},"Industry":{"label":"Industry","value":"Computer Manufacturing"},"OneYrTarget":{"label":"1 Year Target","value":"$272.50"},"TodayHighLow":{"label":"Today's High/Low","value":"$267.43/$264.23"},"ShareVolume":{"label":"Share Volume","value":"21,674,751"},"AverageVolume":{"label":"50 Day Average Vol.","value":"26,839,599"},"PreviousClose":{"label":"Previous Close","value":"$265.76"},"FiftTwoWeekHighLow":{"label":"52 Week High/Low","value":"$267.43/$142.00"},"MarketCap":{"label":"Market Cap","value":"1,186,796,081,500"},"PERatio":{"label":"P/E Ratio","value":22.54},"ForwardPE1Yr":{"label":"Forward P/E 1 Yr.","value":20.280941533788916},"EarningsPerShare":{"label":"Earnings Per Share(EPS)","value":"$11.85"},"AnnualizedDividend":{"label":"Annualized Dividend","value":"$3.08"},"ExDividendDate":{"label":"Ex Dividend Date","value":"Nov 7, 2019"},"DividendPaymentDate":{"label":"Dividend Pay Date","value":"Nov 14, 2019"},"Yield":{"label":"Current Yield","value":"1.1589%"},"Beta":{"label":"Beta","value":1.02}},"assetClass":"STOCKS"},"message":null,"status":{"rCode":200,"bCodeMessage":null,"developerMessage":null}}"""],
                     'hours': ["""{"data":{"symbol":"AAPL","companyName":"Apple Inc. Common Stock","stockType":"Common Stock","exchange":"NASDAQ-GS","isNasdaqListed":true,"isNasdaq100":true,"isHeld":false,"primaryData":{"lastSalePrice":"$266.782","netChange":"0.318","percentageChange":"0.12%","deltaIndicator":"down","lastTradeTimestamp":"DATA AS OF Nov 19, 2019 10:33 AM ET","isRealTime":true},"secondaryData":null,"keyStats":{"Volume":{"label":"Volume","value":"2,492,507"},"PreviousClose":{"label":"Previous Close","value":"$267.10"},"OpenPrice":{"label":"Open","value":"$267.96"},"MarketCap":{"label":"Market Cap","value":"1,185,383,123,230"}},"marketStatus":"Market Open","assetClass":"STOCKS"},"message":null,"status":{"rCode":200,"bCodeMessage":null,"developerMessage":null}}""",
                               """{"data":{"symbol":"AAPL","summaryData":{"Exchange":{"label":"Exchange","value":"NASDAQ-GS"},"Sector":{"label":"Sector","value":"Technology"},"Industry":{"label":"Industry","value":"Computer Manufacturing"},"OneYrTarget":{"label":"1 Year Target","value":"$272.50"},"TodayHighLow":{"label":"Today's High/Low","value":"$268.00/$266.34"},"ShareVolume":{"label":"Share Volume","value":"4,401,591"},"AverageVolume":{"label":"50 Day Average Vol.","value":"26,839,599"},"PreviousClose":{"label":"Previous Close","value":"$267.10"},"FiftTwoWeekHighLow":{"label":"52 Week High/Low","value":"$267.43/$142.00"},"MarketCap":{"label":"Market Cap","value":"1,188,862,199,725"},"PERatio":{"label":"P/E Ratio","value":22.58},"ForwardPE1Yr":{"label":"Forward P/E 1 Yr.","value":20.280941533788916},"EarningsPerShare":{"label":"Earnings Per Share(EPS)","value":"$11.85"},"AnnualizedDividend":{"label":"Annualized Dividend","value":"$3.08"},"ExDividendDate":{"label":"Ex Dividend Date","value":"Nov 7, 2019"},"DividendPaymentDate":{"label":"Dividend Pay Date","value":"Nov 14, 2019"},"Yield":{"label":"Current Yield","value":"1.1589%"},"Beta":{"label":"Beta","value":1.02}},"assetClass":"STOCKS"},"message":null,"status":{"rCode":200,"bCodeMessage":null,"developerMessage":null}}"""],
                     'after hours': ["""{"data":{"symbol":"AAPL","companyName":"Apple Inc. Common Stock","stockType":"Common Stock","exchange":"NASDAQ-GS","isNasdaqListed":true,"isNasdaq100":true,"isHeld":false,"primaryData":{"lastSalePrice":"$267.10","netChange":"0.81","percentageChange":"0.30%","deltaIndicator":"up","lastTradeTimestamp":"DATA AS OF Nov 19, 2019 4:31 PM ET - AFTER HOURS","isRealTime":true},"secondaryData":{"lastSalePrice":"$266.29","netChange":"0.81","percentageChange":"0.3%","deltaIndicator":"down","lastTradeTimestamp":"CLOSED AT 4:00 PM ET ON Nov 19, 2019","isRealTime":false},"keyStats":{"Volume":{"label":"Volume","value":"11,964,129"},"PreviousClose":{"label":"Previous Close","value":"$267.10"},"OpenPrice":{"label":"Open","value":"$267.96"},"MarketCap":{"label":"Market Cap","value":"1,183,197,036,850"}},"marketStatus":"After Hours","assetClass":"STOCKS"},"message":null,"status":{"rCode":200,"bCodeMessage":null,"developerMessage":null}}""",
                                     """{"data":{"symbol":"AAPL","summaryData":{"Exchange":{"label":"Exchange","value":"NASDAQ-GS"},"Sector":{"label":"Sector","value":"Technology"},"Industry":{"label":"Industry","value":"Computer Manufacturing"},"OneYrTarget":{"label":"1 Year Target","value":"$272.50"},"TodayHighLow":{"label":"Today's High/Low","value":"$268.00/$265.39"},"ShareVolume":{"label":"Share Volume","value":"17,612,533"},"AverageVolume":{"label":"50 Day Average Vol.","value":"26,839,599"},"PreviousClose":{"label":"Previous Close","value":"$267.10"},"FiftTwoWeekHighLow":{"label":"52 Week High/Low","value":"$267.43/$142.00"},"MarketCap":{"label":"Market Cap","value":"1,183,197,036,850"},"PERatio":{"label":"P/E Ratio","value":22.47},"ForwardPE1Yr":{"label":"Forward P/E 1 Yr.","value":20.280941533788916},"EarningsPerShare":{"label":"Earnings Per Share(EPS)","value":"$11.85"},"AnnualizedDividend":{"label":"Annualized Dividend","value":"$3.08"},"ExDividendDate":{"label":"Ex Dividend Date","value":"Nov 7, 2019"},"DividendPaymentDate":{"label":"Dividend Pay Date","value":"Nov 14, 2019"},"Yield":{"label":"Current Yield","value":"1.1589%"},"Beta":{"label":"Beta","value":1.02}},"assetClass":"STOCKS"},"message":null,"status":{"rCode":200,"bCodeMessage":null,"developerMessage":null}}"""],
                     'off hours': ["""{"data":{"symbol":"AAPL","companyName":"Apple Inc. Common Stock","stockType":"Common Stock","exchange":"NASDAQ-GS","isNasdaqListed":true,"isNasdaq100":true,"isHeld":false,"primaryData":{"lastSalePrice":"$266.29","netChange":"0.81","percentageChange":"0.3%","deltaIndicator":"down","lastTradeTimestamp":"DATA AS OF Nov 19, 2019","isRealTime":false},"secondaryData":null,"keyStats":{"Volume":{"label":"Volume","value":"19,069,597"},"PreviousClose":{"label":"Previous Close","value":"$267.10"},"OpenPrice":{"label":"Open","value":"$267.96"},"MarketCap":{"label":"Market Cap","value":"1,183,197,036,850"}},"marketStatus":"Market Closed","assetClass":"STOCKS"},"message":null,"status":{"rCode":200,"bCodeMessage":null,"developerMessage":null}}""",
                                   """{"data":{"symbol":"AAPL","summaryData":{"Exchange":{"label":"Exchange","value":"NASDAQ-GS"},"Sector":{"label":"Sector","value":"Technology"},"Industry":{"label":"Industry","value":"Computer Manufacturing"},"OneYrTarget":{"label":"1 Year Target","value":"$272.50"},"TodayHighLow":{"label":"Today's High/Low","value":"$268.00/$265.39"},"ShareVolume":{"label":"Share Volume","value":"19,069,597"},"AverageVolume":{"label":"50 Day Average Vol.","value":"26,839,599"},"PreviousClose":{"label":"Previous Close","value":"$267.10"},"FiftTwoWeekHighLow":{"label":"52 Week High/Low","value":"$267.43/$142.00"},"MarketCap":{"label":"Market Cap","value":"1,183,197,036,850"},"PERatio":{"label":"P/E Ratio","value":22.47},"ForwardPE1Yr":{"label":"Forward P/E 1 Yr.","value":20.280941533788916},"EarningsPerShare":{"label":"Earnings Per Share(EPS)","value":"$11.85"},"AnnualizedDividend":{"label":"Annualized Dividend","value":"$3.08"},"ExDividendDate":{"label":"Ex Dividend Date","value":"Nov 7, 2019"},"DividendPaymentDate":{"label":"Dividend Pay Date","value":"Nov 14, 2019"},"Yield":{"label":"Current Yield","value":"1.1589%"},"Beta":{"label":"Beta","value":1.02}},"assetClass":"STOCKS"},"message":null,"status":{"rCode":200,"bCodeMessage":null,"developerMessage":null}}"""]}

        for k, (info, summary) in test_data.items():
            data = t.stock_data_info_summary(json.loads(info),
                                             json.loads(summary))
            with self.subTest(sub=k):
                if k == 'pre hours':
                    self.assertEqual(data['last'], 267.85)
                    self.assertEqual(data['high'], None)
                    self.assertEqual(data['low'], None)
                    self.assertEqual(data['open'], 265.74)
                    self.assertEqual(data['close'], None)
                    self.assertEqual(data['volume'], 111677.0)
                    self.assertEqual(data['market_cap'], None)
                    self.assertEqual(data['shares'], None)
                    self.assertEqual(data['trade_date'], dt.date(2019, 11, 19))
                    self.assertEqual(data['ticker'], 'AAPL')
                if k == 'hours':
                    self.assertEqual(data['last'], 266.782)
                    self.assertEqual(data['high'], 268.0)
                    self.assertEqual(data['low'], 266.34)
                    self.assertEqual(data['open'], 267.96)
                    self.assertEqual(data['close'], None)
                    self.assertEqual(data['volume'], 2492507.0)
                    self.assertEqual(data['market_cap'], 1185383123230.0)
                    self.assertEqual(data['shares'], 4443265000)
                    self.assertEqual(data['trade_date'], dt.date(2019, 11, 19))
                    self.assertEqual(data['ticker'], 'AAPL')
                if k == 'after hours':
                    self.assertEqual(data['last'], 267.1)
                    self.assertEqual(data['high'], 268.0)
                    self.assertEqual(data['low'], 265.39)
                    self.assertEqual(data['open'], 267.96)
                    self.assertEqual(data['close'], 266.29)
                    self.assertEqual(data['volume'], 11964129.0)
                    self.assertEqual(data['market_cap'], 1183197036850.0)
                    self.assertEqual(data['shares'], 4443265000)
                    self.assertEqual(data['trade_date'], dt.date(2019, 11, 19))
                    self.assertEqual(data['ticker'], 'AAPL')
                if k == 'off hours':
                    self.assertEqual(data['last'], 266.29)
                    self.assertEqual(data['high'], 268.0)
                    self.assertEqual(data['low'], 265.39)
                    self.assertEqual(data['open'], 267.96)
                    self.assertEqual(data['close'], 266.29)
                    self.assertEqual(data['volume'], 19069597.0)
                    self.assertEqual(data['market_cap'], 1183197036850.0)
                    self.assertEqual(data['shares'], 4443265000)
                    self.assertEqual(data['trade_date'], dt.date(2019, 11, 19))
                    self.assertEqual(data['ticker'], 'AAPL')


class TestDownlod(unittest.TestCase):
    def test_get_nasdaq(self):
        nasdaq = t.get_nasdaq()
        columns = set(nasdaq.columns)
        columns_answer = set(['quote', 'company_name',
                              'norm_name', 'market_cap',
                              'sector', 'industry'])
        self.assertEqual(columns, columns_answer)
        self.assertEqual(nasdaq.index.name, 'ticker')


class TestNasdaqSearch(unittest.TestCase):
    def get_cik_by_ticker(self, ticker: str) -> int:
        ticker_cik = {'MSFT': 789019,
                      'AAPL': 320193,
                      'RAPT': 0,
                      'E': 1002242,
                      'I': 1525773}
        return ticker_cik.get(ticker, 0)

    def test(self):
        answer = [('MSFT', 789019, 0),
                  ('AAPL', 320193, 0),
                  ('RAPT', np.nan, 0),
                  ('CPTI', np.nan, 0),
                  ('A', 100, 1),
                  ('B', 200, 1),
                  ('C', 300, 0),
                  ('D', 400, 1),
                  ('E', 1002242, 0),
                  ('F', 500, 1),
                  ('G', np.nan, 1),
                  ('H', 600, 1),
                  ('I', 1525773, 0),
                  ('J', np.nan, 0)]
        test_dir = make_absolute('res/', __file__)
        nasdaq = (pd.read_csv(os.path.join(test_dir, 'nasdaq.csv'), sep='\t')
                  .set_index('ticker'))
        nasdaq_cik = (
            pd.read_csv(
                os.path.join(
                    test_dir,
                    'nasdaq_cik.csv'),
                sep='\t') .set_index('ticker'))
        companies = (
            pd.read_csv(
                os.path.join(
                    test_dir,
                    'companies.csv'),
                sep='\t') .set_index('cik'))

        with unittest.mock.patch('firms.tickers.get_nasdaq') as gn, \
                unittest.mock.patch('firms.tickers.get_nasdaq_cik') as gnc, \
                unittest.mock.patch('firms.tickers.get_companies') as gc, \
                unittest.mock.patch('firms.tickers.get_cik_by_ticker') as gt:
            gn.return_value = nasdaq
            gnc.return_value = nasdaq_cik
            gc.return_value = companies
            gt.side_effect = self.get_cik_by_ticker

            nasdaq = t.attach()

            gt.assert_has_calls(
                [unittest.mock.call('MSFT'),
                 unittest.mock.call('AAPL'),
                 unittest.mock.call('RAPT'),
                 unittest.mock.call('CPTI'),
                 unittest.mock.call('E'),
                 unittest.mock.call('I'),
                 unittest.mock.call('J')])
            self.assertEqual(nasdaq.shape[0], 14)
            self.assertTrue('cik' in nasdaq.columns)
            self.assertTrue('checked' in nasdaq.columns)

            for ticker, cik, checked in answer:
                with self.subTest(ticker=ticker):
                    n_cik = nasdaq.loc[ticker]['cik']
                    n_checked = nasdaq.loc[ticker]['checked']
                    self.assertEqual(n_checked, checked)
                    if pd.isna(n_cik):
                        self.assertTrue(pd.isna(cik))
                    else:
                        self.assertEqual(n_cik, cik)


if __name__ == '__main__':
    unittest.main()
