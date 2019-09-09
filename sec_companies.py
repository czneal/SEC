# -*- coding: utf-8 -*-
"""
Created on Thu Aug 29 17:43:57 2019

@author: Asus
"""
import pandas as pd
import numpy as np
import re
from typing import List, Tuple, Dict, Union
from itertools import product
from bs4 import BeautifulSoup

import queries as q
from mysqlio.basicio import OpenConnection
from mysqlio.xbrlfileio import ReportToDB
from xbrlxml.xbrlrss import SecEnumerator
from xbrldown.download import download_rss
from exceptions import XbrlException
from utils import ProgressBar
from urltools import fetch_urlfile

def get_feature(bs: BeautifulSoup, marker: str) -> str:
    try:
        c = bs.find(text=re.compile('.*' + marker + '.*', re.I))
        feature = c.find_next().text.strip()        
    except AttributeError:
        return ''
    return feature

def get_record_by_cik(cik: int) -> Dict[str, Union[str, int]]:
    url = ('https://www.edgarcompany.sec.gov/servlet/'+
          'CompanyDBSearch?page=detailed&cik={0}'+
          '&main_back=1')
    bs = BeautifulSoup(fetch_urlfile(url.format(cik)), 'lxml')
    markers = {'company_name': 'company\s+name',
               'irs': 'irs\s+number',
               'rep_no': 'reporting\s+file\s+number',
               'entity_type': 'regulated\sentity\s+type',
               'sic': 'sic\s+code',
               'updated': 'date\s+of\s+last\s+update'}
    record = {}
    for field, marker in markers.items():
        record[field] = get_feature(bs, marker)
        if field == 'sic': 
            if record['sic'] != '':
                record['sic'] = int(record['sic'])
            else:
                record['sic'] = 0
    
    return record

def get_company_sec(cik: int) -> Tuple[str, int]:
    url = 'https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={}'
    bs = BeautifulSoup(fetch_urlfile(url.format(cik)), features='lxml')
    try:
        info = bs.find(attrs={'class':'companyName'})
        company_name = re.findall(r'(.+)cik#', info.text, re.I)[0].strip()
    except AttributeError or IndexError:
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

def update_companies(years: List[int], months: List[int],
                     refresh_rss: bool = False) -> None:
    #update rss files
    if refresh_rss:
        print('update rss files')
        for y, m in product(years, months):
            try:
                download_rss(y, m)
            except XbrlException as e:
                print(str(e))
                
    #read data ind update database   
    print('read records from xbrl rss file')
    rss = SecEnumerator(years=years, months=months)
    records = [r for r in rss.filing_records(all_types=True)]
    
    print('write companies to database')
    with OpenConnection() as con:
        cur = con.cursor(dictionary=True)
        
        pb = ProgressBar()
        pb.start(len(records))
        
        for record, _ in records:
            ReportToDB.write_company(cur, record)
            con.commit()
            pb.measure()
            print('\r' + pb.message(), end='')
        print()
        print('ok')

def update_companies_from_sec_fomrs() -> None:
    with OpenConnection() as con:
        cur = con.cursor(dictionary=True)
        cur.execute(q.select_compaines_from_sec_forms)
        df = pd.DataFrame(cur.fetchall()).set_index('cik')
        
    return df

if __name__ == "__main__":
#    df = update_companies_from_sec_fomrs()
#    df['sic'] = np.nan
#    
#    pb = ProgressBar()
#    pb.start(df.shape[0])
#    
#    for cik, row in df.iterrows():
#        company_name, sic = get_company_name_by_cik(cik)
#        if company_name:
#            df.loc[cik, ['company_name', 'sic']] = (company_name, sic)
#        
#        pb.measure()
#        print('\r' + pb.message(), end='')
#    print()
#    
#    df.to_csv('outputs/companies.csv')
    
    data = []
    with OpenConnection() as con:
        cur = con.cursor(dictionary=True)
        cur.execute('select distinct cik from sec_forms where cik not in (select distinct cik from companies)')
        df = pd.DataFrame(cur.fetchall())
        
    pb = ProgressBar()
    pb.start(len(df['cik'].unique()))
    for index, row in df[0:100].iterrows():        
        record = get_company_sec(row['cik'])
        data.append(record)
        
        pb.measure()
        print('\r' + pb.message(), end='')
        
    print()
        
    df = pd.DataFrame(data)
    df.to_csv('outputs/companies_search_sec.csv')
    
    
    
    
    
    
    