# -*- coding: utf-8 -*-
import itertools
from typing import List

import xbrldown.download as dwn
from xbrlxml.xbrlrss import SecEnumerator
from xbrlxml.xbrlexceptions import XbrlException
from utils import ProgressBar
from log_file import Logs
from settings import Settings

def download(years: List[int], months: List[int]) -> None:
    logs = Logs(Settings.root_dir(), append_log=True, name='down_log')
    
    logs.log('download rss file(s)...')
    for year, month in itertools.product(years, months):
        try:
            dwn.download_rss(year, month) # raise
        except XbrlException as e:
            logs.set_header([year, year])
            logs.error(str(e))
    logs.log('download rss file(s)...ok')
        
    sec = SecEnumerator(years, months)
    records = list(sec.filing_records())
    
    pb = ProgressBar()
    pb.start(len(records))
    for record, zip_filename in records:
        logs.set_header([record['cik'], record['adsh'], zip_filename])
        
        try:
            dwn.check_zip_file(zip_filename)
            dwn.check_zip_file_deep(zip_filename)
            
            logs.log('ok')
        except XbrlException:
            try:
                logs.log('check fails try to download')
                dwn.download_and_save(
                        int(record['cik']), record['adsh'],
                        zip_filename)
            except XbrlException as e:
                logs.error(str(e))
                pass
        pb.measure()
        print('\r' + pb.message(), end='')
        
    print()
    

if __name__ == '__main__':
    download(years=[2019], months=[1, 8, 12])
