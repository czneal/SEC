# -*- coding: utf-8 -*-
"""
Created on Fri Aug 30 17:50:53 2019

@author: Asus
"""

import pandas as pd
import numpy as np
import re
import datetime as dt

from bs4 import BeautifulSoup
from typing import List

import queries as q
from utils import ProgressBar
from urltools import fetch_urlfile
from mysqlio.basicio import OpenConnection
from mysqlio.basicio import Table


def get_cik_by_ticker(ticker: str) -> int:
    bs = BeautifulSoup(
            fetch_urlfile(
                    'https://www.sec.gov/cgi-bin/browse-edgar?CIK={}'
                       .format(ticker)), 'lxml')
    
    link = bs.find('a', {'href': re.compile(r'CIK=')})
    if link is not None:
        m = re.findall('\d{10}', link['href'])
        if m:
            return int(m[0])
    
    return 0

def last_price(ticker: str) -> float:
    url = 'https://www.nasdaq.com/symbol/{0}'.format(ticker)
    bs = BeautifulSoup(fetch_urlfile(url),'lxml')
    price = np.nan
    high = np.nan
    low = np.nan
    try:
        price_text = bs.find('div', {'id': 'qwidget_lastsale'}).text
        price = float(re.findall(r'\d+\.?\d*', price_text)[0])
        
        high_low = bs.find(text=re.compile('.*today.+high.+low.*', re.I))
        p = high_low.find_next('div', 
                               text=re.compile('.*\d+.?\d*\s+/.*\d+.?\d*.*')).text
        pp = re.findall(r'(\d+\.?\d*)', p)
        high = float(pp[0])
        low = float(pp[1])
        
    except AttributeError:
        pass
    except IndexError:
        pass
    
    return price, high, low


def split_company_name(company_name: str) -> List[str]:
    company_name = (company_name
                    .lower()
                    .replace('.com', ' com')
                    .replace('corporation', 'corp')
                    .replace('company', 'co')
                    .replace('incorporated', 'inc')
                    .replace('limited', 'ltd')
                    .replace('the', ''))
    company_name = re.sub('\.+|\'+', '', company_name)
    company_name = re.sub('/.{2,5}$', '', company_name)
    company_name = re.sub('\&\#\d+;', '', company_name)
    symbols = {',':' ','.':'','/':' ','&':' ','-':' ',
               '\\':' ','\'':'','(':' ',')':' ','#':' ',
               ':':' ','!':' ', ' ': ' '}
               
    parts = [company_name]
    for symbol in symbols:
        new_parts = []
        for part in parts:
            new_parts.extend([p.strip().lower() for p in part.split(symbol)
                                            if p.strip()])
        parts = new_parts
        
    return parts

def distance(name1: str, name2: str) -> float:
    words1 = set(split_company_name(name1))
    words2 = set(split_company_name(name2))
    
    return len(words1.intersection(words2))*2/(len(words1) + len(words2))
    
def cap(market_cap: str) -> float:
    if pd.isna(market_cap):
        return np.nan
    
    value = re.findall(r'\d+\.*\d*', market_cap)
    if not value:
        return np.nan
    value = float(value[0])
    mult = 1.0
    if 'm' in market_cap.lower():
        mult = 1000000
    if 'b' in market_cap.lower():
        mult = 1000000000
        
    return value*mult

def get_nasdaq() -> pd.DataFrame:
    """
    return pandas DataFrame
    symbol (ticker) as index
    columns
    'quote' - hyperlink to nasdaq web site
    'company_name' - company name
    'norm_name' - cleared company name
    'market_cap' - company capitalization (can be NAN)    
    'sector' - company sector (can be NAN)
    'industry' - company industry (can be NAN)    
    """
    url_text = ("https://www.nasdaq.com/screening/companies-by-name.aspx?" + 
                "letter=0&exchange={0}&render=download")
    frames = []
    for exchange in ['nasdaq', 'nyse', 'amex']:
        frames.append(
                pd.read_csv(
                        fetch_urlfile(url_text.format(exchange)),
                        converters={'Symbol': str.strip,
                                    'Name': str.strip}))
    nasdaq = (pd.concat(frames)
                .drop_duplicates('Symbol')
                .rename(columns={'Symbol': 'ticker',
                                 'Name': 'company_name',                             
                                 'Sector': 'sector',
                                 'industry': 'industry',                                 
                                 'Summary Quote': 'quote'})
                .set_index('ticker'))
    
    nasdaq['market_cap'] = nasdaq['MarketCap'].apply(cap)
    nasdaq['norm_name'] = nasdaq['company_name'].apply(
            lambda x: ' '.join(split_company_name(x)))
    nasdaq = nasdaq[~(nasdaq['norm_name'].str.contains(r'\s+etf'))]
    
    return nasdaq[['quote', 'company_name', 'norm_name', 
                   'market_cap', 'sector', 'industry']]

def get_nasdaq_changes() -> pd.DataFrame:
    """
    return pandas DataFrame
    columns: 
    'new_ticker' - new ticker name
    'old_ticker' - old ticker_name
    'sdate' - effective date
    """
    
    url = ("https://www.nasdaq.com/markets/stocks/"+
          "symbol-change-history.aspx?page={0}")
    frames = []    
    for page in [1,2,3,4]:
        tables = pd.read_html(fetch_urlfile(url.format(page)))
        for table in tables:
            if table.shape[0] > 0 and table.shape[1]>1:
                frames.append(table)
    
    changes = (pd.concat(frames)
                 .rename(columns={'Old Symbol': 'old_ticker',
                                  'New Symbol': 'new_ticker',
                                  'Effective Date': 'sdate'})
                 .dropna(axis='index', how='any'))
    changes['sdate'] = pd.to_datetime(changes['sdate'], 
                                      infer_datetime_format=True)
    return changes

def write_nasdaq_changes(changes: pd.DataFrame, 
                         last_update: dt.datetime) -> None:
    upd = changes[changes['sdate']>last_update]
    with OpenConnection() as con:
        cur = con.cursor(dictionary=True)
        cur.executemany(q.update_nasdaq_changes, 
                        upd[['new_ticker', 'old_ticker']].to_dict('records'))
        con.commit()

def get_nasdaq_cik() -> pd.DataFrame:
    with OpenConnection() as con:
        cur = con.cursor(dictionary=True)
        cur.execute('select * from nasdaq')
        nasdaq_cik = pd.DataFrame(cur.fetchall())
        if nasdaq_cik.shape[0] == 0:
            nasdaq_cik = pd.DataFrame(columns = ['ticker', 'company_name',
                                                 'cik', 'quote', 'industry',
                                                 'sector', 'checked',
                                                 'market_cap'])
        
    return nasdaq_cik.set_index('ticker')

def get_companies(active_from: dt.date) -> pd.DataFrame:
    with OpenConnection() as con:
        cur = con.cursor(dictionary=True)
        cur.execute('select c.cik, c.company_name from companies c ' +
                    "where updated>=%(date)s", {'date': active_from})
        companies = pd.DataFrame(cur.fetchall()).set_index('cik')
        companies['norm_name'] = companies['company_name'].apply(
            lambda x: ' '.join(split_company_name(x)))
        companies.drop_duplicates(subset='norm_name', 
                                  keep=False, 
                                  inplace=True)
    return companies

def write_nasdaq(nasdaq: pd.DataFrame) -> None:
    with OpenConnection() as con:
        cur = con.cursor(dictionary=True)
        table = Table('nasdaq', con=con)
        mapping = {'ticker': 'ticker',
                   'company_name': 'company_name',
                   'sector': 'sector',
                   'industry': 'industry',
                   'market_cap': 'market_cap',
                   'quote': 'quote',
                   'cik': 'cik',
                   'checked': 'checked'}
        cur.execute('truncate table nasdaq')
        table.write_df(nasdaq.reset_index().rename(columns=mapping), cur)
        con.commit()
        
def search() -> pd.DataFrame:
    nasdaq = get_nasdaq()
    companies = get_companies(dt.date(2018,9,4))
    nasdaq_cik = get_nasdaq_cik()
    
    
    #attach cik to new nasdaq symbols
    nasdaq = pd.merge(nasdaq, nasdaq_cik[['company_name', 'cik', 'checked']], 
                 how='left', 
                 left_index=True, right_index=True,
                 suffixes=('', '_y'))
    
    #if old company name doesnt match new one we should perfom new search
    w = ((nasdaq['company_name'] != nasdaq['company_name_y']) &
         (nasdaq['company_name_y'].notna()))
    nasdaq.loc[w, ['cik', 'checked']] = (np.nan, 0)
    
    #if not checked before perfom new search
    nasdaq.loc[nasdaq['checked'].isna(), ['cik', 'checked']] = (np.nan, 0)
    
    #perfom full match only where cik is NAN
    new = nasdaq[nasdaq['cik'].isna()]
    new = new.drop(axis='columns', labels=['cik', 'company_name_y'])
    
    #merge with companies to find cik
    new = (pd.merge(new.reset_index(), companies.reset_index(),
                   how='left',
                   left_on='norm_name', right_on='norm_name',
                   suffixes=('', '_y'))    
             .set_index('ticker'))   
    #if company name full match we dont have to check it
    new.loc[new['cik'].notna(), 'checked'] = 1
    
    #search SEC for cik by ticker in this case we should check
    pb = ProgressBar()
    pb.start(new[new['checked'] == 0].shape[0])
    
    for ticker, row in new[new['checked'] == 0].iterrows():
        cik = get_cik_by_ticker(ticker)        
        if cik:
            new.loc[ticker, ['cik', 'checked']] = (cik, 0)
        pb.measure()
        print('\r' + pb.message(), end='')
    print()
    
    nasdaq.update(new[new['cik'].notna()][['cik', 'checked']])
    
    return nasdaq.drop(axis='columns', labels=['norm_name', 'company_name_y'])
    

if __name__ == '__main__':    
    nasdaq = search()
    write_nasdaq(nasdaq)
    
    with OpenConnection() as con:
        cur = con.cursor(dictionary=True)
        cur.execute("""select ticker, n.company_name as n_name,
                            c.cik as cik, c.company_name as c_name
                    from nasdaq n, companies c where n.cik = c.cik""")
        df = pd.DataFrame(cur.fetchall())
        df['dist'] = df[['n_name', 'c_name']].apply(
                lambda x: distance(x['n_name'], x['c_name']),
                axis=1)
        
        f = df[df['dist']<0.9]
