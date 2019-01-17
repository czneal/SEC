# -*- coding: utf-8 -*-
"""
Created on Tue Jan  8 15:23:57 2019

@author: Asus
"""

import urltools
from bs4 import BeautifulSoup
import zipfile
import os
from settings import Settings
import sys
import xbrl_scan


def check_zip_files(y, m):
    print('year:{0} month:{1}'.format(y, m))

    for (dirpath, dirnames, filenames) in os.walk(Settings.root_dir() +
                             str(y)+"/"+str(m).zfill(2)):
        for filename in filenames:
            if not filename.endswith('.zip'):
                continue

            if not check_zip_file(dirpath + '/' + filename):
                print('check fail for:{0}'.format(dirpath + '/' + filename))

#for y in range(2017, 2018):
#    for m in range(2, 3):
#        #check_zip_files(y, m)
#        xbrl_scan.update_current_month(y, m)

