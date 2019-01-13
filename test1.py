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


def fetch_report_files(filename):
    try:
        parts = filename.split('/')[-1].split('.')[0]
        pos = parts.find('-')
        cik = int(parts[0:pos])
        adsh_sep = parts[pos+1:]
        adsh = adsh_sep.replace('-', '')

        url = 'https://www.sec.gov/Archives/edgar/data/{0}/{1}/{2}-index.htm'.format(cik, adsh, adsh_sep)
        soup = BeautifulSoup(urltools.fetch_urlfile(url), 'lxml')

        files = {}
        name = ""
        for node in soup.find_all('a'):
            link = node.get('href')
            if link.endswith('.xsd'):
                files['xsd'] = link
                continue
            if link.endswith('_cal.xml'):
                files['cal'] = link
                continue
            if link.endswith('_def.xml'):
                continue
            if link.endswith('_lab.xml'):
                files['lab'] = link
                continue
            if link.endswith('_pre.xml'):
                files['pre'] = link
                continue
            if link.endswith('_ref.xml'):
                continue

            if link.endswith('.xml'):
                files['xbrl']= link

        if len(files) != 5:
            print('unable fetch files for:{0}'.format(filename))
            if 'xsd' not in files: print('xsd missing')
            if 'pre' not in files: print('pre missing')
            if 'cal' not in files: print('cal missing')
            if 'lab' not in files: print('lab missing')
            if 'xbrl' not in files: print('xbrl missing')
            return False

        zfile = zipfile.ZipFile(filename, 'w')
        for name, link in files.items():
            byteio = bytes(urltools.fetch_urlfile('https://www.sec.gov' + link).read())
            zfile.writestr(link.split('/')[-1], byteio)

        zfile.close()

        print('file repaired:{0}'.format(filename))

    except:
        print(sys.exc_info())
        return False

    return True

def check_zip_file(filename):
    try:
        zfile = zipfile.ZipFile(filename, 'r')
        namelist = zfile.namelist()
        zfile.close()
    except:
        print('try to repair:{0}'.format(filename))
        return fetch_report_files(filename)

    try:
        files = [f for f in namelist if f.endswith('.xml')]

        xbrl_name = ''
        for name in files:
            if not (name.endswith('_pre.xml') or
                name.endswith('_lab.xml') or
                name.endswith('_cal.xml') or
                name.endswith('_def.xml') or
                name.endswith('_ref.xml')):
                xbrl_name = name

        if xbrl_name == '':
            fetch_report_files(filename)
    except:
        print(sys.exc_info())
        return False

    return True

def check_zip_files(y, m):
    print('year:{0} month:{1}'.format(y, m))

    for (dirpath, dirnames, filenames) in os.walk(Settings.root_dir() +
                             str(y)+"/"+str(m).zfill(2)):
        for filename in filenames:
            if not filename.endswith('.zip'):
                continue

            if not check_zip_file(dirpath + '/' + filename):
                print('check fail for:{0}'.format(dirpath + '/' + filename))

for y in range(2017, 2018):
    for m in range(2, 3):
        #check_zip_files(y, m)
        xbrl_scan.update_current_month(y, m)