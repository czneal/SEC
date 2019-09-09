# -*- coding: utf-8 -*-

import unittest, unittest.mock
import os
import pandas as pd
import numpy as np
from unittest.mock import call

import tickers
from utils import add_app_dir

class TestDownlod(unittest.TestCase):
    def test_get_nasdaq(self):
        nasdaq = tickers.get_nasdaq()
        columns = set(nasdaq.columns)
        columns_answer = set(['quote', 'company_name', 
                              'norm_name', 'market_cap', 
                              'sector', 'industry'])
        self.assertEqual(columns, columns_answer)
        self.assertEqual(nasdaq.index.name, 'ticker')

class TestNasdaqSearch(unittest.TestCase):
    def get_cik_by_ticker(self, ticker: str) -> int:
        ticker_cik = {'MSFT':789019,
                   'AAPL':320193,
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
        test_dir = add_app_dir('tests/resources/tickers/')
        nasdaq = (pd.read_csv(os.path.join(test_dir, 'nasdaq.csv'), sep='\t')
                   .set_index('ticker'))
        nasdaq_cik = (pd.read_csv(os.path.join(test_dir, 'nasdaq_cik.csv'), sep='\t')
                        .set_index('ticker'))
        companies = (pd.read_csv(os.path.join(test_dir, 'companies.csv'), sep='\t')
                      .set_index('cik'))
        
        with unittest.mock.patch('tickers.get_nasdaq') as gn, \
             unittest.mock.patch('tickers.get_nasdaq_cik') as gnc, \
             unittest.mock.patch('tickers.get_companies') as gc, \
             unittest.mock.patch('tickers.get_cik_by_ticker') as gt:
                 gn.return_value = nasdaq
                 gnc.return_value = nasdaq_cik
                 gc.return_value = companies
                 gt.side_effect = self.get_cik_by_ticker
                 
                 nasdaq = tickers.search()
                 
                 gt.assert_has_calls([call('MSFT'), call('AAPL'), 
                                      call('RAPT'), call('CPTI'),
                                      call('E'), call('I'), call('J')])
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