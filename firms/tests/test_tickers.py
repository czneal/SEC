import unittest
import json
import datetime as dt

import firms.tickers as t


class TestStockData(unittest.TestCase):
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


if __name__ == '__main__':
    unittest.main()
