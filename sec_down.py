# -*- coding: utf-8 -*-
import itertools
from typing import List

import xbrldown.download as dwn
from xbrlxml.xbrlrss import SecEnumerator
from xbrlxml.xbrlexceptions import XbrlException
from utils import ProgressBar
from log_file import Logs
from settings import Settings

def download(years: List[int], 
             months: List[int], 
             append_log: bool,
             refresh_rss: bool) -> None:
    logs = Logs(Settings.root_dir(), append_log=append_log, name='down_log')
    
    if refresh_rss:
        logs.log('download rss file(s)...')
        for year, month in itertools.product(years, months):
            try:
                dwn.download_rss(year, month)
            except XbrlException as e:
                logs.set_header([year, month])
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
        except XbrlException as e:
            try:
                logs.log('check fails, try to download')
                logs.error(str(e))				
                dwn.download_and_save(
                        int(record['cik']), record['adsh'],
                        zip_filename)
                logs.log('ok')
            except XbrlException as e:
                logs.log('fail')
                logs.error(str(e))
                pass
        pb.measure()
        print('\r' + pb.message(), end='')
        
    print()
    

if __name__ == '__main__':
    download(years=[y for y in range(2015,2016)], 
             months=[m for m in range(1,13)], 
             append_log=False,
             refresh_rss=False)
	
	#dwn.check_zip_file_deep('/mnt/md0/sec/2014/04/0000912615-0001193125-14-125999.zip')
