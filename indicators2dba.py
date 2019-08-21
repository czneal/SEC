# -*- coding: utf-8 -*-
"""
Created on Wed Apr  3 18:12:42 2019

@author: Asus
"""
import pandas as pd

from mysql.connector.errors import InternalError

import mysqlio.basicio as do
from indi.indpool import IndicatorsPool
from utils import retry

@retry(retry=5, exc_cls=InternalError)
def write_mg_descr(ind_pool: IndicatorsPool) -> None:
    with do.OpenConnection() as con:
        desc = do.Table('mgparamsdesc', con)
        cur = con.cursor(dictionary=True)
        for ind_name, ind in ind_pool.indicators.items():
            desc.write({'description':ind.description(), 
                        'pname':ind_name}, cur)
        desc.flush(cur)
        con.commit()
        
@retry(retry=5, exc_cls=InternalError)
def write_gaap_descr(gaap_tags):
    with do.OpenConnection() as con:
        cursor = con.cursor()
        format_strings = ','.join(['%s'] * len(gaap_tags))
        cmd = """insert into mgparamsdesc
                       select concat(version, ':', tag), docum.text from docum
                       where concat(version, ':', tag) in ({0})
                           and  type = '{1}'
                       on duplicate key update
                       description = docum.text, type = 1"""
        cursor.execute(cmd.format(format_strings, 'label'), 
                       tuple(gaap_tags))
        cursor.execute(cmd.format(format_strings, 'documentation'), 
                       tuple(gaap_tags))

        con.commit()

@retry(retry=5, exc_cls=InternalError)
def write_params(params: pd.DataFrame) -> None:
    with do.OpenConnection() as con:
        cursor = con.cursor(dictionary=True)
        columns_mapping = {
                'adsh':'adsh', 
                'fy': 'fy',
                'pname': 'pname',
                'sname': 'sname',
                'o': 'offs', 
                'w': 'weight', 
                'class': 'class_id', 
                'ord': 'ord', 
                'value': 'value',
                'sadsh': 'sadsh'}
        params.rename(columns_mapping, axis='columns', inplace=True)
        
        table = do.Table('mgparamstype1', con)
        table.write_df(params, cursor)
        table.flush(cursor)
        con.commit()
        
@retry(retry=5, exc_cls=InternalError)
def write_mg_nums(nums: pd.DataFrame) -> None:
    with do.OpenConnection() as con:
        n = nums[nums['name'].str.startswith('mg_')]
        assert id(n) != id(nums)
        
        n = (n.dropna(subset=['value'], axis='index')
              .fillna(value={'version': ''}, axis='index'))
        n['uom'] = ''
        n['type'] = 'C'
        columns_mapping = {
                'adsh':'adsh',
                'tag': 'tag',
                'version': 'version',
                'fy': 'fy',
                'value': 'value',
                'uom':'uom',
                'type': 'type'}
        nums.rename(columns_mapping, axis='columns', inplace=True)
        
        cursor = con.cursor(dictionary=True)
        table = do.Table(con=con, name='mgnums')
        table.write_df(n, cursor)
        
        con.commit()
    
@retry(retry=5, exc_cls=InternalError)
def write_report(nums, cik, adsh):
    with do.OpenConnection() as con:
        cursor = con.cursor(dictionary=True)
        table = do.Table('mgreporttable', con)
        n = nums[(nums['tag'].isin(table.fields)) & (nums['adsh'] == adsh)]
        n = n.pivot(index='adsh', columns='tag', values='value').reset_index()
        n['cik'] = cik
        table.write_df(n, cursor)
        table.flush(cursor)
        con.commit()
    