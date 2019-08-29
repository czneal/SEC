# -*- coding: utf-8 -*-
"""
Created on Mon Aug 19 15:09:02 2019

@author: Asus
"""


import datetime
import os

import sec_parse
import sec_down
import sec_companies
from xbrlxml.xbrlrss import SecEnumerator, CustomEnumerator
from settings import Settings

if __name__ == '__main__':
    d = datetime.datetime.now()

    #download new reports for current month    
    sec_down.download(years=[d.year], months=[d.month],
                      append_log=True,
                      refresh_rss=True)
    
    #update companies
    sec_companies.update_companies(years=[d.year], months=[d.month],
                                   refresh_rss=False)
    
    #read unsuccessfull reports from repeat.rss
    repeat_filename = os.path.join(Settings.root_dir(),'repeat.rss')
    
    repeat_records = []
    if os.path.exists(repeat_filename):
        rss_repeat = CustomEnumerator(repeat_filename)
        repeat_records = rss_repeat.filing_records()
    
    #read reports from sec rss file
    rss_sec = SecEnumerator([d.year], [d.month])
    records = sec_parse.concat_records(list(rss_sec.filing_records()),
                                       repeat_records)
    
    #parse them into database
    sec_parse.read(records=records,
                   repeat=repeat_filename,
                   slice_=slice(0, None),
                   log_dir=Settings.root_dir(),
                   append_log=True)