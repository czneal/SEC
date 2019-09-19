# -*- coding: utf-8 -*-
import pandas as pd
import datetime as dt

import queries as q
from firms.utils import split_company_name
from mysqlio.basicio import OpenConnection
from mysqlio.basicio import Table
from mysqlio.xbrlfileio import ReportToDB

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

def get_new_companies() -> pd.DataFrame:
    with OpenConnection() as con:
        cur = con.cursor(dictionary=True)
        cur.execute(q.select_new_companies)
        df = pd.DataFrame(cur.fetchall())
    if df.shape[0] == 0:
        df['cik'] = 0
        
    return df

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
        
def write_companies(companies: pd.DataFrame) -> None:
    """
    companies columns: ['cik', 'company_name', 'sic', 'updated']
    write companies to database
    write new
    update exested if 'updated' in companies >= than 'updated' report.companies
    """
    mapping = {'updated': 'file_date'}
    companies = companies.rename(columns=mapping)
    with OpenConnection() as con:
        cur = con.cursor(dictionary=True)
        for index, row in companies.iterrows():
            ReportToDB.write_company(cur, row)
        con.commit()
            
def write_sec_forms(sec_forms: pd.DataFrame) -> None:
    with OpenConnection() as con:
        table = Table('sec_forms', con)
        cur = con.cursor(dictionary=True)
        table.write_df(sec_forms, cur)        
        con.commit()
    