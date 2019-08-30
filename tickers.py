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

from utils import ProgressBar
from urltools import fetch_urlfile
from mysqlio.basicio import OpenConnection

def get_cik_by_ticker(ticker: str) -> int:
    bs = BeautifulSoup(
            fetch_urlfile(
                    'https://www.sec.gov/cgi-bin/browse-edgar?CIK={}'
                       .format(ticker)), 'lxml')
    
    links = bs.find_all('a', {'href': re.compile(r'CIK=')})
    if links:
        m = re.findall('\d{10}', links[0]['href'])
        if m:
            return int(m[0])
    
    return 0

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
    url_text = ("https://www.nasdaq.com/screening/companies-by-name.aspx?" + 
                "letter=0&exchange={0}&render=download")
    frames = []
    for exchange in ['nasdaq', 'nyse', 'amex']:
        frames.append(
                pd.read_csv(
                        fetch_urlfile(url_text.format(exchange))))
    nasdaq = pd.concat(frames).drop_duplicates('Symbol').set_index('Symbol')
    
    nasdaq['cap'] = nasdaq['MarketCap'].apply(cap)
    nasdaq['norm_name'] = nasdaq['Name'].apply(
            lambda x: ' '.join(split_company_name(x)))
    nasdaq = nasdaq[~(nasdaq['norm_name'].str.contains(r'\s+etf'))]
    
    return nasdaq.dropna(axis=1, how='all')

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

if __name__ == '__main__':
    print('read tickers and companies')
    nasdaq = get_nasdaq()
    companies = get_companies(dt.date(2018, 6, 1))
    
    print('assign')
    nasdaq = pd.merge(nasdaq.reset_index(), companies.reset_index(),
                 how='left',
                 right_on='norm_name',
                 left_on='norm_name',
                 suffixes=('', '_y')).set_index('Symbol')
    
    non_cik = nasdaq[nasdaq['cik'].isna()].sort_values('cap', ascending=False)
    
    f = non_cik.iloc[:10]
    pb = ProgressBar()
    pb.start(f.shape[0])
    
    print('scrape SEC for ticker-cik pair')
    for symbol, row in f.iterrows():
        cik = get_cik_by_ticker(symbol)
        if cik != 0:
            nasdaq.loc[symbol, 'cik'] = cik
        pb.measure()
        print('\r' + pb.message(), end='')
    print()
