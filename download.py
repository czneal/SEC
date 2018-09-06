# -*- coding: utf-8 -*-
"""
Created on Fri Aug 04 16:48:04 2017

@author: Asus
"""

import urllib
import urllib.request
import datetime as dt
import xml.etree.ElementTree as ET
import zipfile
import os
import shutil
import sys
import traceback

class ScraperLogFile(object):
    def __init__(self, filename):
        self.log_file = None
        if os.path.exists(filename):
            self.log_file = open(filename,"a")            
        else:
            self.log_file = open(filename, "w")
            
        self.write("session timestamp {0}".format(dt.date.today()))
        
    def write(self, info, end="\n"):
        self.log_file.write(str(info)+end)
    def close(self):
        self.log_file.close()
        
def SECdownload(year, month, part_dir, log):
    
    root_link = "https://www.sec.gov/Archives/edgar/monthly/"
    rss_filename = "xbrlrss-" + str(year) + "-" + str(month).zfill(2) + ".xml"
    link = root_link + rss_filename
    rss_filename = rss_filename[4:]
    
    if DownloadFile(link, part_dir + rss_filename, log):
        return part_dir + rss_filename
    else:
        return ""
  
def ItemFilesDownload(item, target_dir, temp_dir, ns, log):
    
    xbrl_section = item.find("edgar:xbrlFiling", ns)
    cik_id = xbrl_section.find("edgar:cikNumber", ns).text
    access_id = xbrl_section.find("edgar:accessionNumber", ns).text
    filer = xbrl_section.find("edgar:companyName", ns).text
    
    zip_filename = target_dir+"/"+cik_id+"-"+access_id+".zip"
    if os.path.exists(zip_filename):
        #log.write("\t" + zip_filename + " exist!")
        return
    try:
        log.write("\tDownloading %s cik %s" %(filer, cik_id))         
        enc = item.find("enclosure")
        one_file_url = enc.get("url")      
        if DownloadFile(one_file_url, zip_filename, log):
            log.write("\tZip file downloaded!")
        else:
            log.write("Fail!")
    
    except:
        all_files_read = True
        
        zfile = zipfile.ZipFile(zip_filename,"w")
        
        files_section = xbrl_section.find("edgar:xbrlFiles", ns)
        files = files_section.findall("edgar:xbrlFile", ns)
        for f in files:
           file_url = f.get("{"+ns["edgar"]+"}"+"url")
           filename = file_url.split("/")[-1]
           if not DownloadFile(file_url, temp_dir + filename, log):
               all_files_read = False
               break
           else:
               zfile.write(temp_dir + filename, filename)
        zfile.close()
        zfile = None
        
        shutil.rmtree(temp_dir)
        os.mkdir(temp_dir)
        
        if not all_files_read:
            os.remove(zip_filename)
            log.write("Fail!")
        else:
            log.write("\tSuccess!")
            
    return
def DownloadFile(url_text, filename, log):
    
    tryout = 3
    good_read = False
    body = None
    
    while tryout>0:
        try:
            req = urllib.request.Request(url_text)
            url = urllib.request.urlopen(req)
            body = url.read()
            url.close()
            good_read = True
            break
        except:
            log.write("\t%d attempt of downloading %s fail" %(4-tryout, url_text))
            log.write(sys.exc_info())
            tryout = tryout - 1
    if not good_read:        
       log.write("Couldn't download file %s" % url_text)
    else:
        of = open(filename, "wb")
        of.write(body)
        of.close()
    
    return good_read

def CreatePartialDir(year, month, root_dir):
    if not os.path.exists(root_dir + str(year)):
        os.mkdir(root_dir + str(year))
    if not os.path.exists(root_dir + str(year) + "/" + str(month).zfill(2)):
        os.mkdir(root_dir + str(year) + "/" + str(month).zfill(2))
    return root_dir + str(year) + "/" + str(month).zfill(2) + "/"

def global_downloader():
    #ns = {'edgar': 'http://www.sec.gov/Archives/edgar',
    #      'atom': 'http://www.w3.org/2005/Atom'}
    try:
        root_dir = "d:/SEC/"
        temp_dir = root_dir + "tmp/"
        if not os.path.exists(root_dir):
            os.mkdir(root_dir)
        if not os.path.exists(temp_dir):
            os.mkdir(temp_dir)
            
        for y in range(2017, 2019):
            for m in range(1,13):
                print("year:{0}, month:{1}".format(y, m))
                part_dir = CreatePartialDir(y, m, root_dir)            
                rss_filename = SECdownload(y, m, part_dir)
                
                if rss_filename == "":               
                    continue           
                
                events = ("start-ns", "start")
                ns = {}
                c = ET.iterparse(rss_filename, events=events)
                c = iter(c)
                root = None
                #parse namespace names
                for event, elem in c:
                    if event == "start-ns":
                        ns[elem[0].lower()] = elem[1]
                    if event == "start" and root == None: 
                        root = elem        
                        
                items = root.iter("item")
                for item in items:
                    ItemFilesDownload(item, part_dir, temp_dir, ns)
            
        
    except:
        print("Something went wrong!")
        print(sys.exc_info())

def download_one_month(m,y,log,err_log):
    try:
        root_dir = "d:/SEC/"
        temp_dir = root_dir + "tmp/"
        if not os.path.exists(root_dir):
            os.mkdir(root_dir)
        if not os.path.exists(temp_dir):
            os.mkdir(temp_dir)
        
        log.write("year:{0}, month:{1}".format(y, m))
        part_dir = CreatePartialDir(y, m, root_dir)            
        rss_filename = SECdownload(y, m, part_dir, log)
        
        if rss_filename == "":               
            return
        
        events = ("start-ns", "start")
        ns = {}
        c = ET.iterparse(rss_filename, events=events)
        c = iter(c)
        root = None
        #parse namespace names
        for event, elem in c:
            if event == "start-ns":
                ns[elem[0].lower()] = elem[1]
            if event == "start" and root == None: 
                root = elem
                
        items = root.iter("item")
        for item in items:
            ItemFilesDownload(item, part_dir, temp_dir, ns, log)            
        
    except:
        err_log.write("Something went wrong!")
        err_log.write(sys.exc_info()[0])        
        err_log.write(sys.exc_info()[1])
        traceback.print_tb(sys.exc_info()[2], file=err_log.log_file)
        
def update_current_month():
    try:
        y = dt.date.today().year
        m = dt.date.today().month
        if dt.date.today().day == 1:
            m -= 1
        if m == 0:
            m = 12
            y -= 1
        
        log = ScraperLogFile("d:/sec/scraper_log.txt")
        err_log = ScraperLogFile("d:/sec/scraper_err_log.txt")
        download_one_month(m,y,log,err_log)
        log.close()
        err_log.close()
    except:
        log.close()
        err_log.close()

