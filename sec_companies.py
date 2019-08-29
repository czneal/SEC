# -*- coding: utf-8 -*-
"""
Created on Thu Aug 29 17:43:57 2019

@author: Asus
"""
from typing import List
from itertools import product

from mysqlio.basicio import OpenConnection
from mysqlio.xbrlfileio import ReportToDB
from xbrlxml.xbrlrss import SecEnumerator
from xbrldown.download import download_rss
from exceptions import XbrlException
from utils import ProgressBar

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
    print('read records')
    rss = SecEnumerator(years=years, months=months)
    records = [r for r in rss.filing_records(all_types=True)]
    
    print('write to database')
    with OpenConnection() as con:
        cur = con.cursor(dictionary=True)
        
        pb = ProgressBar()
        pb.start(len(records))
        
        for record, _ in records:
            ReportToDB.write_company(cur, record)
            con.commit()
            pb.measure()
            print('\r' + pb.message(), end='')

if __name__ == "__name__" :
    update_companies(years=[2019], months=[1,2,3,4,5,6,7], refresh_rss=True)
        
