# -*- coding: utf-8 -*-
"""
Created on Mon Sep 10 15:07:08 2018

@author: Media
"""
import pandas as pd

from xbrlxml.dataminer import SharesDataMiner, ChapterNamesMiner
from xbrlxml.xbrlrss import CustomEnumerator
from log_file import Logs, RepeatFile
from mysqlio.xbrlfileio import ReportToDB
from mysqlio.basicio import OpenConnection
from utils import ProgressBar
from utils import remove_root_dir

if __name__ == '__main__':
    try:        
        rss = CustomEnumerator('outputs/shares.csv')
        records = rss.filing_records()
        
        logs = Logs(log_dir='outputs/', name='shares')
        repeat = RepeatFile('outputs/repeatrss.csv')
        
        dm = ChapterNamesMiner(logs=logs, repeat=repeat)
        r2db = ReportToDB(logs=logs, repeat=repeat)        
        
        data = []
        with OpenConnection() as con:
            pb = ProgressBar()
            pb.start(len(records))
            
            print()
            
            cur = con.cursor(dictionary=True)
            for record, zip_filename in records[927:928]:
                logs.set_header([record['cik'], remove_root_dir(zip_filename)])
                repeat.set_state(record, remove_root_dir(zip_filename))
                
                dm.feed(record, zip_filename)
                df = dm.shares_facts
                r2db.write_shares(cur, record, dm)
                con.commit()
                pb.measure()
                print('\r' + pb.message(), end='')
            print()
    except:
        raise
    finally:
        if 'logs' in locals(): logs.close()
        if 'repeat' in locals(): repeat.close()
#    try:
#        rss = CustomEnumerator('outputs/shares_test.csv')
#        records = rss.filing_records()
#        
#        logs = Logs(log_dir='outputs/', name='shares')
#        repeat = RepeatFile('outputs/repeatrss.csv')
#        xbrlfile = XbrlFile()
#        
#        for record, zip_filename in records:
#            logs.set_header([record['cik'], zip_filename])
#            repeat.set_state(record, zip_filename)
#            try:
#                xbrlfile.read(record, )
#            except XbrlException as e:
#                logs.error(str(e))
#                repeat.repeat()
#            except Exception:
#                logs.traceback()
#                repeat.repeat()
#    except:
#        raise
#    finally:
#        if 'logs' in locals():
#            logs.close()
#        if 'repeat' in locals():
#            repeat.close()