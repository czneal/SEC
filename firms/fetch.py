# -*- coding: utf-8 -*-
import re
import os
import pandas as pd

from typing import List, Dict, Union, Optional, cast

from bs4 import BeautifulSoup
from multiprocessing import Manager, Pool
from multiprocessing.synchronize import Lock
from multiprocessing.managers import SyncManager

from utils import ProgressBar
from urltools import fetch_urlfile
from firms.futils import cap, split_company_name

class LockStub(Lock):
    def __init__(self):
        pass

    def acquire(self):
        pass
            
    def release(self):
        pass
            
def find_company_attr(cik: int) -> Dict[str, Union[str, int]]:
    url = 'https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={}'
    bs = BeautifulSoup(fetch_urlfile(url.format(cik)), features='lxml')
    try:
        info = bs.find(attrs={'class':'companyName'})
        company_name = re.findall(r'(.+)cik#', info.text, re.I)[0].strip()
    except (AttributeError, IndexError):
        company_name = ''
    try:
        sic = bs.find('a', href = re.compile('&sic', re.I)).text
    except AttributeError:
        sic = ''
        
    if sic == '':
        sic = 0
    else:
        sic = int(sic)
        
    return {'cik': cik,
            'company_name': company_name, 
            'sic': sic}

def companies_search(ciks: List[int],
                     lock: Lock = None,                       
                     pid: int = 0) -> pd.DataFrame:
    data = []
    
    pb = ProgressBar()
    pb.start(len(ciks))
    
    for index, cik in enumerate(ciks):
        data.append(find_company_attr(cik))
        pb.measure()
        
        if lock: lock.acquire()
        try:
            print('\r{0}: {1}'.format(str(pid).zfill(2), pb.message()), end='')
        finally:
            if lock: lock.release()
    
    print()    
    return pd.DataFrame(data)

def companies_search_mpc(ciks: List[int], n_procs: int=7) -> pd.DataFrame:
    cpus = min(n_procs, len(ciks))
    if cpus == 0:
        df = pd.DataFrame([], columns=['cik', 'company_name', 'sic'])
        return df
        
    print('run {0} proceses'.format(cpus))        
        
    with Manager() as m, Pool(cpus) as p:
        m = cast(SyncManager, m)
        lock = m.Lock()
        records_per_cpu = int(len(ciks)/cpus) + 1
        params = []
        for i, start in enumerate(range(0, len(ciks), records_per_cpu)):
            params.append([ciks[start: start + records_per_cpu], lock, i+1])
        
        frames = p.starmap(companies_search, params)
        if frames:
            df = pd.concat(frames, ignore_index=True, sort=False)
        else:
            df = pd.DataFrame([], columns=['cik', 'company_name', 'sic'])
    return df
    
def get_cik_by_ticker(ticker: str) -> int:
    bs = BeautifulSoup(
            fetch_urlfile(
                    'https://www.sec.gov/cgi-bin/browse-edgar?CIK={}'
                       .format(ticker)), 'lxml')
    
    link = bs.find('a', {'href': re.compile(r'CIK=')})
    if link is not None:
        m = re.findall(r'\d{10}', link['href'])
        if m:
            return int(m[0])
    
    return 0

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
    url_text = ("https://old.nasdaq.com/screening/companies-by-name.aspx?" + 
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

def get_sec_forms(year: int, quarter: int) -> pd.DataFrame:
    """
    download crawler.idx file from SEC server for year and quarter    
    return pandas DataFrame with columns:
        'cik', 
        'company_name', 
        'form' - SEC form type,
        'filed',
        'doc_link' - link to html page with filing elements
    """
    f = fetch_urlfile(url_text='https://www.sec.gov/Archives/edgar/full-index/'+
                               '{0}/QTR{1}/crawler.idx'.format(year, quarter))
    
    if f is None:
        return pd.DataFrame(columns=['company_name', 'form', 
                                     'cik', 'filed', 'doc_link',
                                     'adsh', 'owner'])
    
    eated = False
    columns: List[int] = []
    r = re.compile(r"(?P<name>company name)\s+(?P<type>form type)\s+(?P<cik>cik)\s+(?P<date>date filed)\s+(?P<url>url.*)", 
                   re.IGNORECASE)
    data = []
    for line in f.readlines():
        try:
            line = line.decode()
        except UnicodeDecodeError:
            line = line.decode('cp1250')
            
        line = line.replace('\n', '')
        if eated:
            row = []
            for i in range(len(columns)-1):
                row.append(line[columns[i]:columns[i+1]].strip())                
            data.append(row)
            continue

        g = r.match(line)
        if not eated and g:
            eated = True
            columns = [g.start('name'), g.start('type'),
                       g.start('cik'), g.start('date'),
                       g.start('url'), -1]
            continue
    
    df = pd.DataFrame(data[1:], columns=['company_name', 'form', 
                                     'cik', 'filed', 'doc_link'])
    df = df.drop_duplicates(subset=['doc_link'])
    df = df[df['form'] !=  'UPLOAD']
    df = df.astype({'cik': int})
    
    df['adsh'] = df['doc_link'].apply(lambda x: re.findall(r'\d{10}-\d{2}-\d{6}', x)[0])
    df['owner'] = df['adsh'].apply(lambda x: int(re.findall(r'(\d{10})-', x)[0]))
    
    #replace technical owner by cik
    df.loc[df['owner']>2147483647, 'owner'] = df['cik']    
    df.loc[~(df['owner'].isin(df['cik'].unique())), 'owner'] = df['cik']
    
    return df
    
# =============================================================================
# by this methods not all companies can by fetched
#
# def get_feature(bs: BeautifulSoup, marker: str) -> str:
#         try:
#             c = bs.find(text=re.compile('.*' + marker + '.*', re.I))
#             feature = c.find_next().text.strip()        
#         except AttributeError:
#             return ''
#         return feature
# 
# def get_record_by_cik(cik: int) -> Dict[str, Union[str, int]]:
#     url = ('https://www.edgarcompany.sec.gov/servlet/'+
#           'CompanyDBSearch?page=detailed&cik={0}'+
#           '&main_back=1')
#     bs = BeautifulSoup(fetch_urlfile(url.format(cik)), 'lxml')
#     markers = {'company_name': 'company\s+name',
#                'irs': 'irs\s+number',
#                'rep_no': 'reporting\s+file\s+number',
#                'entity_type': 'regulated\sentity\s+type',
#                'sic': 'sic\s+code',
#                'updated': 'date\s+of\s+last\s+update'}
#     record = {}
#     for field, marker in markers.items():
#         record[field] = get_feature(bs, marker)
#         if field == 'sic': 
#             if record['sic'] != '':
#                 record['sic'] = int(record['sic'])
#             else:
#                 record['sic'] = 0
#     
#     return record
# =============================================================================

if __name__ == '__main__':
    df = get_nasdaq()
    i=22