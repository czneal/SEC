# -*- coding: utf-8 -*-
"""
Created on Mon Sep  2 11:34:17 2019

@author: Asus
"""

import re
import pandas as pd
import itertools
from typing import List

import mysqlio.basicio as do
from urltools import fetch_urlfile

def get_crawler(year: int, quarter: int) -> pd.DataFrame:
    f = fetch_urlfile(url_text='https://www.sec.gov/Archives/edgar/full-index/'+
                               '{0}/QTR{1}/crawler.idx'.format(year, quarter))
    
    if f is None:
        return pd.DataFrame(columns=['company_name', 'form', 
                                     'cik', 'filed', 'doc_link',
                                     'adsh', 'owner'])
    
    eated = False
    columns = []
    data = []
    for line in f.readlines():
        try:
            line = line.decode()
        except UnicodeDecodeError:
            line = line.decode('cp1250')
            
            
        line = line.replace(u'\n', u'')
        if eated:
            row = []
            for i in range(len(columns)-1):
                row.append(line[columns[i]:columns[i+1]].strip())                
            data.append(row)
            
        if not eated and re.match(u'company name\s+form type\s+cik\s+date filed\s+url.*', 
                                  line, re.I):
            eated = True
            columns.append(0)                
            columns.append(re.search(u'form type', line, re.I).start())
            columns.append(re.search(u'cik', line, re.I).start())
            columns.append(re.search(u'date filed', line, re.I).start())
            columns.append(re.search(u'url', line, re.I).start())
            columns.append(None)
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

def update_sec_forms(years:List[int], months: List[int]) -> None:
    quarters = set([int((m + 2)/3) for m in months])
    for y, q in itertools.product(years, quarters):
        print('get crawler file for year: {0}, quarter: {1}'.format(y, q))
        forms = get_crawler(y, q)
        print('ok')
        
        print('write sec forms for year: {0}, quarter: {1} to database'
              .format(y, q))
        with do.OpenConnection() as con:
            table = do.Table('sec_forms', con)
            cur = con.cursor(dictionary=True)
            table.write_df(forms, cur)        
            con.commit()
        print('ok')
        
if __name__ == '__main__':
    #update_sec_forms(years=range(2012, 2020), months=range(1, 13))
    update_sec_forms(years=range(2017, 2020), months=range(1, 13))
        