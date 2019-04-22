# -*- coding: utf-8 -*-
"""
Created on Mon Sep 10 15:07:08 2018

@author: Media
"""

import scraper_sec
from settings import Settings
from log_file import LogFile

log = LogFile(Settings.root_dir() + 'sec_dwn.log')
err = LogFile(Settings.root_dir() + 'sec_dwn.err')
for y in range(2012, 2013):
	for m in range(1,13):
		print('year:{0}, month:{1}'.format(y, m))
		scraper_sec.download_one_month(y, m, log=log, err_log=err)

log.close()
err.close()