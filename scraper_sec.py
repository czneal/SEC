# -*- coding: utf-8 -*-
"""
Created on Fri Aug 04 16:48:04 2017

@author: Asus
"""

#import urllib
#import urllib.request
import datetime as dt
import xml_tools
import zipfile
import os
import sys
import log_file
from settings import Settings
import urltools
from bs4 import BeautifulSoup
from utils import ProgressBar

def download_xbrlrss(year, month, part_dir, log=log_file.LogFile()):
    root_link = "https://www.sec.gov/Archives/edgar/monthly/"
    rss_filename = "xbrlrss-" + str(year) + "-" + str(month).zfill(2) + ".xml"
    link = root_link + rss_filename
    rss_filename = rss_filename[4:]

    return urltools.fetch_urlfile(link, part_dir + rss_filename, log)

def fetch_report_files(filename, log=log_file.LogFile()):
    try:
        log.write('try to fetch files for:{0}'.format(filename))

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
            if 'xsd' not in files: log.write('xsd missing')
            if 'pre' not in files: log.write('pre missing')
            if 'cal' not in files: log.write('cal missing')
            if 'lab' not in files: log.write('lab missing')
            if 'xbrl' not in files: log.write('xbrl missing')
            if 'xsd' not in files or 'pre' not in files or 'xbrl' not in files:
                log.write('unable fetch files for {0}'.format(filename))
                return False

        zfile = zipfile.ZipFile(filename, 'w')
        for name, link in files.items():
            byteio = bytes(urltools.fetch_urlfile('https://www.sec.gov' + link).read())
            zfile.writestr(link.split('/')[-1], byteio)
        zfile.close()

        log.write('files fetched:{0}'.format(filename))
    except:
        log.write('unable fetch files for: {0}'.format(filename))
        log.write_tb(sys.exc_info())
        return False

    return True

def check_zip(filename, log=log_file.LogFile()):

    try:
        good = True
        zfile = zipfile.ZipFile(filename, 'r')
        namelist = zfile.namelist()
        files = [f for f in namelist if f.endswith('.xml')]
        for file in files:
            try:
                tree = xml_tools.XmlTreeTools()
                tree.read_xml_tree(zfile.open(file))
            except:
                good = False
        zfile.close()
    except:
        return False

    if not good: return False

    try:
        xbrl_name = ''
        for name in files:
            if not (name.endswith('_pre.xml') or
                name.endswith('_lab.xml') or
                name.endswith('_cal.xml') or
                name.endswith('_def.xml') or
                name.endswith('_ref.xml')):
                xbrl_name = name

        if xbrl_name == '':
            return False
        else:
            return True

    except:
        log.write_tb(sys.exc_info())
        return False

def repair_zip(filename, log=log_file.LogFile()):
    log.write('checking zip file: {0}'.format(filename))
    if check_zip(filename, log):
        log.write('zip file good')
        return True

    log.write('zip file not good, try to repair')
    if not fetch_report_files(filename, log):
        return False

    return check_zip(filename, log)

def download_zip(url, filename, log=log_file.LogFile()):
    try:
        log.write('try to download: {0}'.format(url))
        if urltools.fetch_urlfile(url, filename, log) != filename:
            log.write('unable download: {0}'.format(url))
        else:
            log.write('zip file downloaded: {0}'.format(filename))

    except:
        log.write('unable download: {0}'.format(url))
        log.write_tb(sys.exc_info())

def ItemFilesDownload(item, target_dir, temp_dir, ns, overwrite, check,
                      log=log_file.LogFile()):

    xbrl_section = item.find("edgar:xbrlFiling", ns)
    cik_id = xbrl_section.find("edgar:cikNumber", ns).text
    access_id = xbrl_section.find("edgar:accessionNumber", ns).text
    filer = xbrl_section.find("edgar:companyName", ns).text

    zip_filename = target_dir + cik_id + "-" + access_id+".zip"
    if overwrite or not os.path.exists(zip_filename):
        try:
            log.write("downloading %s cik %s" %(filer, cik_id))
            enc = item.find("enclosure")
            one_file_url = enc.get("url")
            download_zip(one_file_url, zip_filename, log)
        except:
            log.write('link to zip file not found')

    if check or overwrite:
        if not repair_zip(zip_filename, log):
            log.write('zip file unrepairable: {0}'.format(zip_filename))
            return False

    return True

def CreatePartialDir(year, month, root_dir):
    if not os.path.exists(root_dir + str(year)):
        os.mkdir(root_dir + str(year))
    if not os.path.exists(root_dir + str(year) + "/" + str(month).zfill(2)):
        os.mkdir(root_dir + str(year) + "/" + str(month).zfill(2))
    return root_dir + str(year) + "/" + str(month).zfill(2) + "/"

def download_one_month(y, m, log=log_file.LogFile(), err_log=log_file.LogFile()):
    try:
        root_dir = Settings.root_dir()
        temp_dir = root_dir + "tmp/"
        if not os.path.exists(root_dir):
            os.mkdir(root_dir)
        if not os.path.exists(temp_dir):
            os.mkdir(temp_dir)

        log.write("year:{0}, month:{1}".format(y, m))
        part_dir = CreatePartialDir(y, m, root_dir)
        log.write('download new rss xbrl file')
        rss_filename = download_xbrlrss(y, m, part_dir, log)

        if rss_filename == "":
            return

        tree = xml_tools.XmlTreeTools()
        tree.read_xml_tree(rss_filename)
        root = tree.root

        items = list(root.iter("item"))
        pb = ProgressBar()
        pb.start(total = len(items))
        for item in items:
            ItemFilesDownload(item, part_dir, temp_dir, tree.ns,
                              overwrite = False, check=True, log=log)
            pb.measure()
            print('\r' + pb.message(), end='')
        print()

    except:
        err_log.write("Something went wrong!")
        err_log.write_tb(sys.exc_info())

def update_current_month(y=None, m=None):
    try:
        if y is None or m is None:
            y = dt.date.today().year
            m = dt.date.today().month
            if dt.date.today().day == 1:
                m -= 1
            if m == 0:
                m = 12
                y -= 1

        log = log_file.LogFile(Settings.root_dir() + "scraper_log.txt", append=True)
        err_log = log_file.LogFile(Settings.root_dir() + "scraper_err_log.txt", append=True)
        download_one_month(y, m, log, err_log)
        log.close()
        err_log.close()
    except:
        log.close()
        err_log.close()

def cure_filler_file(filename):
    try:
        parts = filename.split('/')[-1].split('.')[0]
        pos = parts.find('-')
        cik = int(parts[0:pos])
        adsh = parts[pos+1:].replace('-', '')
        url = 'https://www.sec.gov/Archives/edgar/data/{0}/{1}/'.format(cik, adsh)

        zfile = zipfile.ZipFile(filename, mode='r')
        namelist = zfile.namelist()
        zfile.close()

        zfile = zipfile.ZipFile(filename, 'w')
        for file in namelist:
            byteio = bytes(urltools.fetch_urlfile(url + file).read())
            zfile.writestr(file, byteio)

        zfile.close()
    except:
        raise
        return False

    return True

def total_scan():
    for y in range(2013, 2020):
        for m in range(1, 13):
            update_current_month(y, m)

if __name__ == '__main__':
    fetch_report_files('z:/sec/2013/09/0000086759-0001144204-13-051352.zip')
#err = log_file.LogFile(Settings.output_dir() + 'err.log', append=False)
#log = log_file.LogFile(Settings.output_dir() + 'log.log', append=False)
#
#download_one_month(2013, 3)
#
#err.close()
#log.close()

