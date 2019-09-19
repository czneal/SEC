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

from utils import ProgressBar
from urltools import fetch_urlfile
from mysqlio.firmsio import get_companies, get_nasdaq_cik
from firms.fetch import get_cik_by_ticker, get_nasdaq

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


def attach() -> pd.DataFrame:
    print('get nasdaq symbols, companies...')
    nasdaq = get_nasdaq()
    companies = get_companies(dt.date(2018,9,4))
    nasdaq_cik = get_nasdaq_cik()
    print('ok')
    
    #attach cik to new nasdaq symbols from ones in database
    nasdaq = pd.merge(nasdaq, nasdaq_cik[['company_name', 'cik', 'checked']], 
                 how='left', 
                 left_index=True, right_index=True,
                 suffixes=('', '_y'))
    
    #if old company_name doesnt match new one we should perfom new search
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
    

if __name__ == '__main__':    
    pass
