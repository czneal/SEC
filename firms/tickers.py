# -*- coding: utf-8 -*-
"""
Created on Fri Aug 30 17:50:53 2019

@author: Asus
"""

import pandas as pd # typing: ignore
import numpy as np
import re
import datetime as dt
import json

from bs4 import BeautifulSoup #typing: ignore
from typing import Tuple, Dict, Optional, Union, cast

from utils import ProgressBar
from urltools import fetch_urlfile, fetch_with_delay
from mysqlio.firmsio import get_companies, get_nasdaq_cik
from firms.fetch import get_cik_by_ticker, get_nasdaq
from firms.futils import convert_date, convert_decimal, select_data, date_from_timestamp, stock_type


def period(todate: dt.date = dt.datetime.now().date(),
           days: int = 365) -> Tuple[dt.date, dt.date, int]:
    fromdate = todate - dt.timedelta(days=days)
    limit = np.busday_count(fromdate, todate)

    return todate, fromdate, limit


def historical_data(
        ticker: str,
        todate: dt.date = dt.datetime.now().date(),
        days: int = 365) -> pd.DataFrame:
    todate, fromdate, limit = period(todate, days)
    # 'https://api.nasdaq.com/api/quote/QQQ/historical?assetclass=etf&fromdate=2019-09-03&limit=5&todate=2019-10-03'

    url = ('https://api.nasdaq.com/api/quote/{0}/historical?' +
           'assetclass={4}&fromdate={1}&' +
           'limit={2}&todate={3}')
    df = pd.DataFrame(columns=['ticker',
                               'close', 'high', 'open', 'low',
                               'volume', 'trade_date'])
    try:
        for type_ in {'stocks', 'etf'}:
            body = fetch_with_delay(url.format(ticker,
                                               fromdate,
                                               limit,
                                               todate,
                                               type_))
            if body is None:
                continue
            response = json.loads(body)
            if response['data'] is None:
                body = fetch_with_delay(url.format(ticker,
                                                   fromdate,
                                                   limit + 1,
                                                   todate,
                                                   type_))
                if body is None:
                    continue
                response = json.loads(body)
            if response['data'] is None:
                continue

            df = (pd.DataFrame(response['data']['tradesTable']['rows'])
                    .apply(stocks_convert, axis=1)
                    .rename(columns={'date': 'trade_date'}))
            df['ticker'] = ticker
            break
    finally:
        return df


def historical_dividents(ticker: str) -> pd.DataFrame:
    url = ('https://api.nasdaq.com/api/quote/{0}/' +
           'dividends?assetclass={1}')

    df = pd.DataFrame(columns=['ticker',
                               'ex_eff_date', 'type', 'amount',
                               'declaration_date', 'record_date',
                               'payment_date'])
    try:
        for type_ in {'stocks', 'etf'}:
            body = fetch_with_delay(url.format(ticker, type_))
            if body is None:
                continue
            response = json.loads(body)
            if response['data'] is None:
                continue

            df = (pd.DataFrame(response['data']['dividends']['rows'])
                    .apply(dividents_convert, axis=1)
                    .dropna(axis=0, subset=['paymentDate'])
                    .rename(columns={'exOrEffDate': 'ex_eff_date',
                                     'declarationDate': 'declaration_date',
                                     'recordDate': 'record_date',
                                     'paymentDate': 'payment_date'}))
            df['ticker'] = ticker
    finally:
        return df


StockData = Dict[str, Union[str, float, dt.date, None]]


def stock_data(ticker: str) -> StockData:
    """    
    return dictionary for requested ticker
    {'last': None or last price, when market closed equal close
    'high': None or high price, when premarket is None
    'low': None, same as high
    'open': None or open price,
    'close': None or close price, when premarket or open market is None
    'volume': volume, None if no day tradings
    'market_cap': None or market capitalization, when premarket is None
    'shares': None or shares count, when premarket is None
    'class': None or share class 
    'type': None or share type,
    'ticker': equal requested ticker,
    'trade_date': None or trade date,
    'ttype': None or share unified type,
    'market_status': None or 'Pre Market', 'Market Open', 'After Hours', 'Market Close'}
    """
    url = ('https://api.nasdaq.com/api/quote/{0}/{1}?assetclass={2}')

    retval = stock_data_info_summary({}, {})
    retval['ticker'] = ticker

    try:
        for type_ in ['stocks', 'etf']:
            b1 = fetch_with_delay(url.format(ticker, 'info', type_))
            b2 = fetch_with_delay(url.format(ticker, 'summary', type_))
            if b1 is None or b2 is None:
                continue

            info = json.loads(b1)
            summary = json.loads(b2)
            if info['data'] is None and summary['data'] is None:
                continue

            retval = stock_data_info_summary(info, summary)
            retval['ticker'] = ticker

    except Exception:
        pass

    return retval


def stock_data_info_summary(info: dict, summary: dict) -> StockData:
    """
    see help for stock_data
    """
    retval: StockData = {'last': None,
                         'high': None,
                         'low': None,
                         'open': None,
                         'close': None,
                         'volume': None,
                         'market_cap': None,
                         'shares': None,
                         'class': None,
                         'type': None,
                         'ticker': None,
                         'trade_date': None,
                         'ttype': None,
                         'market_status': None}
    retval['ticker'] = select_data(info, ['data', 'symbol'])

    primary_date = date_from_timestamp(select_data(
        info, ['data', 'primaryData', 'lastTradeTimestamp']))
    secondary_date = date_from_timestamp(select_data(
        info, ['data', 'secondaryData', 'lastTradeTimestamp']))
    market_status = select_data(
        info, ['data', 'marketStatus'])
    retval['market_status'] = market_status
    hours: int = 0  # off hours
    if market_status == 'Pre Market':
        hours = 1
    elif market_status == 'After Hours':
        hours = 3
    elif market_status == 'Market Open':
        hours = 2
    else:
        hours = 0  # Market Close

    retval['trade_date'] = primary_date
    retval['last'] = convert_decimal(
        select_data(info, ['data', 'primaryData', 'lastSalePrice']))
    retval['open'] = convert_decimal(
        select_data(info, ['data', 'keyStats', 'OpenPrice', 'value']))
    retval['volume'] = convert_decimal(
        select_data(info, ['data', 'keyStats', 'Volume', 'value']))

    if hours != 1:
        high_low = select_data(
            summary, ['data', 'summaryData', 'TodayHighLow', 'value'])
        if high_low is not None:
            high, low = high_low.split('/')
            retval['high'] = convert_decimal(high)
            retval['low'] = convert_decimal(low)
        retval['close'] = convert_decimal(
            select_data(info, ['data', 'secondaryData', 'lastSalePrice']))

        retval['market_cap'] = convert_decimal(
            select_data(info, ['data', 'keyStats', 'MarketCap', 'value']))
        if retval['market_cap'] is None and hours == 0:
            retval['market_cap'] = convert_decimal(select_data(
                summary, ['data', 'summaryData', 'MarketCap', 'value']))

    if hours == 0:
        retval['close'] = retval['last']

    if (hours != 1 and
            retval['market_cap'] is not None and
            retval['last'] is not None and
            retval['last'] != 0.0):
        if hours == 3:
            price = retval['close']
        else:
            price = retval['last']
        retval['shares'] = round(cast(float, retval['market_cap']) /
                                 cast(float, price))

    class_ = select_data(info, ['data', 'assetClass'])
    if class_ is not None:
        retval['class'] = re.sub(
            '\t+|\r+|\n+', '', class_).strip().lower()
    tp = select_data(info, ['data', 'stockType'])
    if tp is not None:
        retval['type'] = re.sub('\t+|\r+|\n+', '', tp).strip()
        retval['ttype'] = stock_type(tp)

    return retval


def last_price(ticker: str) -> Tuple[float, float, float]:
    'https://api.nasdaq.com/api/quote/AAPL/info?assetclass=stocks'
    'https://api.nasdaq.com/api/quote/AAPL/dividends?assetclass=stocks'
    'https://api.nasdaq.com/api/quote/AAPL/historical?assetclass=stocks&fromdate=2009-09-01&limit=1000&todate=2019-10-01'
    url = 'https://old.nasdaq.com/symbol/{0}'.format(ticker)
    bs = BeautifulSoup(fetch_urlfile(url), 'lxml')
    price = np.nan
    high = np.nan
    low = np.nan
    try:
        price_text = bs.find('div', {'id': 'qwidget_lastsale'}).text
        price = float(re.findall(r'\d+\.?\d*', price_text)[0])

        high_low = bs.find(text=re.compile('.*today.+high.+low.*', re.I))
        p = high_low.find_next('div', text=re.compile(
            '.*\d+.?\d*\s+/.*\d+.?\d*.*')).text
        pp = re.findall(r'(\d+\.?\d*)', p)
        high = float(pp[0])
        low = float(pp[1])

    except AttributeError:
        pass
    except IndexError:
        pass

    return price, high, low


def attach() -> pd.DataFrame:
    nasdaq = get_nasdaq()
    companies = get_companies(dt.date(2018, 9, 4))
    nasdaq_cik = get_nasdaq_cik()

    # attach cik to new nasdaq symbols from ones in database
    nasdaq = pd.merge(nasdaq, nasdaq_cik[['company_name', 'cik', 'checked']],
                      how='left',
                      left_index=True, right_index=True,
                      suffixes=('', '_y'))

    # if old company_name doesnt match new one we should perfom new search
    w = ((nasdaq['company_name'] != nasdaq['company_name_y']) &
         (nasdaq['company_name_y'].notna()))
    nasdaq.loc[w, ['cik', 'checked']] = (np.nan, 0)

    # if not checked before perfom new search
    nasdaq.loc[nasdaq['checked'].isna(), ['cik', 'checked']] = (np.nan, 0)

    # perfom full match only where cik is NAN
    new = nasdaq[nasdaq['cik'].isna()]
    new = new.drop(axis='columns', labels=['cik', 'company_name_y'])

    # merge with companies to find cik
    new = (pd.merge(new.reset_index(), companies.reset_index(),
                    how='left',
                    left_on='norm_name', right_on='norm_name',
                    suffixes=('', '_y'))
           .set_index('ticker'))
    # if company name full match we dont have to check it
    new.loc[new['cik'].notna(), 'checked'] = 1

    # search SEC for cik by ticker in this case we should check
    pb = ProgressBar()
    pb.start(new[new['checked'] == 0].shape[0])

    print('search SEC for new tickers')
    for ticker, row in new[new['checked'] == 0].iterrows():
        cik = get_cik_by_ticker(ticker)
        if cik:
            new.loc[ticker, ['cik', 'checked']] = (cik, 0)
        pb.measure()
        print('\r' + pb.message(), end='')
    print()

    nasdaq.update(new[new['cik'].notna()][['cik', 'checked']])

    return nasdaq.drop(axis='columns', labels=['norm_name', 'company_name_y'])


def dividents_convert(row: pd.Series) -> pd.Series:
    row['amount'] = convert_decimal(row['amount'])
    row['declarationDate'] = convert_date(row['declarationDate'])
    row['exOrEffDate'] = convert_date(row['exOrEffDate'])
    row['paymentDate'] = convert_date(row['paymentDate'])
    row['recordDate'] = convert_date(row['recordDate'])

    return row


def stocks_convert(row: pd.Series) -> pd.Series:
    row['close'] = convert_decimal(row['close'])
    row['high'] = convert_decimal(row['high'])
    row['low'] = convert_decimal(row['low'])
    row['open'] = convert_decimal(row['open'])
    row['volume'] = convert_decimal(row['volume'])
    row['date'] = convert_date(row['date'])

    return row


if __name__ == '__main__':
    print(stock_data('wfc'))
