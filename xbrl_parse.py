# -*- coding: utf-8 -*-
"""
Created on Mon Apr 15 17:50:34 2019

@author: Asus
"""

import xbrl_file_ext as x
from log_file import LogFile
import xml_tools as xt
import datetime as dt
from settings import Settings
import os
import pandas as pd
import database_operations as do
import sys
import xbrl_file_v2 as xf
from utils import ProgressBar


class FilingRSS(object):
    def __init__(self):
        self.tree = None        
        return
    
    def open_file(self, filename):
        self.__init__()
        
        self.tree = xt.XmlTreeTools()
        self.tree.read_xml_tree(filename)
                    
        return
    
    def filing_records(self):
        root = self.tree.root        
        records = []
        for item in root.findall(".//item"):
            r = FilingRecord()
            r.read(item)
            if r.form_type in {'10-K','10-K/A'}:
                records.append(r.asdict())
                
        return records
    
    
class FilingRecord(object):
    __attribs = ['company_name', 'form_type', 'cik', 'adsh', 'period', 'file_date', 'fye', 'fy']
    def __init__(self):
        self.company_name = None
        self.form_type = None
        self.cik = None
        self.adsh = None
        self.period = None
        self.file_date = None
        self.fye = None
        self.fy = None
        
    def read(self, record):
        self.__init__()
        for e in record.iter():
            name = e.tag.lower().split('}')[-1]
            if name == 'companyname':
                self.company_name = e.text.strip()
            if name == 'formtype':
                self.form_type = e.text.strip()
            if name == 'ciknumber':
                self.cik = int(e.text)
            if name == 'accessionnumber':
                self.adsh = e.text.strip()
            if name == 'period':
                text = e.text.strip()
                self.period = dt.date(int(text[0:4]), int(text[4:6]), int(text[6:8]))
            if name == 'fiscalyearend':
                self.fye = e.text.strip()
            if name == 'filingdate':
                parts = e.text.strip().split('/')
                self.file_date = dt.date(int(parts[2]), int(parts[0]), int(parts[1]))
                
        if self.period is not None:
            self.fy = (self.period - dt.timedelta(days=365/2)).year
        return
    
    def __str__(self):
        return self.aslist().__str__()
    
    def aslist(self):
        return [getattr(self, a) for a in self.__attribs]
    
    def asdict(self):
        return {a:getattr(self, a) for a in self.__attribs}

def read_dei_section(filename, err = LogFile()):
    data = {'fye':None, 'period':None, 'fy':None}
    try:
        from xbrl_file import XBRLZipPacket
        zfile = XBRLZipPacket(filename)
        zfile.open_packet()
        
        tree = xt.XmlTreeTools()
        tree.read_xml_tree(zfile.xbrl_file)
        root = tree.root
        ns = tree.ns
                
        node = root.find("./dei:CurrentFiscalYearEndDate", ns)
        if node is not None:
            data['fye'] = node.text.strip().replace("-","")
        
        node = root.find("./dei:DocumentPeriodEndDate", ns)
        if node is not None:
            ddate = node.text.strip().replace('-', '')
            data['period'] = dt.date(int(ddate[0:4]), int(ddate[4:6]), int(ddate[6:8]))
                    
        node = root.find("./dei:DocumentFiscalYearFocus", ns)
        if node is not None:
            try:
                data['fy'] = int(node.text.strip())
            except ValueError:
                data['fy'] = None
            if data['fy'] is not None and (data['fy'] < 2000 or data['fy'] > 2100):
                data['fy'] = None
        
    except:
        err.write_tb2(filename, sys.exc_info())
    return data

def update_formtype():
    try:
        con = do.OpenConnection()        
        cur = con.cursor(dictionary=True)
        rss = FilingRSS()
        update = "update raw_reps set form_type =%(form_type)s where adsh=%(adsh)s"
        for y in range(201,2020):
            for m in range(1, 13):              
                parent_dir = Settings.root_dir()+str(y)+"/"+str(m).zfill(2)+'/'
                rss_filename = "rss-" + str(y)+"-"+str(m).zfill(2) + ".xml"
                if not os.path.exists(parent_dir + rss_filename):
                    continue
                
                print('year:{0}, month:{1}'.format(y, m))
                rss.open_file(parent_dir + rss_filename)
                records = rss.filing_records()
                cur.executemany(update, records)
                
        con.commit()
    except:
        raise
    finally:
        if 'con' in locals() and con is not None: con.close()
        
#update_formtype()

        
        
f = FilingRSS()
log = LogFile(Settings.output_dir() + 'read.log', append=True)
err = LogFile(Settings.output_dir() + 'read.err', append=True)
warn = LogFile(Settings.output_dir() + 'read.warn', append=True)
rep = xf.XBRLFile(log, err, warn)


years = [2017]
months = range(1,13) 
#months = [3]

for y in years:
    for m in months:
        parent_dir = Settings.root_dir()+str(y)+"/"+str(m).zfill(2)+'/'
        rss_filename = "rss-" + str(y)+"-"+str(m).zfill(2) + ".xml"
        if not os.path.exists(parent_dir + rss_filename):
            continue
        
        print('year:{0}, month:{1}'.format(y, m))
        f.open_file(parent_dir + rss_filename)        
        records = f.filing_records()
        
        pb = ProgressBar()
        pb.start(len(records))
        
        adsh_stop = False
        for rss_data in records:
            if rss_data['adsh'] != '0001644406-17-000024' and adsh_stop:
                continue                      
            if not rep.read(parent_dir + str(rss_data['cik']).zfill(10) + 
                   '-' + rss_data['adsh'] + '.zip',
                   rss_data):
                continue
                
            try:
                con = do.OpenConnection()
                cur = con.cursor(dictionary=True)
                rw = do.ReportWriter(con)
                rw.write_raw_report(rep, cur)
                rw.write_raw_facts(rep, cur)
                rw.write_raw_contexts(rep, cur)                
                con.commit()
            except:
                raise
            finally:
                if 'con' in locals() and con is not None: con.close()
                
            print('\r' + pb.message(), end='')
            pb.measure()
        print()
            
err.close()
log.close()
warn.close()
            
        

#df = pd.DataFrame(data,columns=['company_name','form','cik','adsh','period','fye', 'file_date'])


#data = []
#err = LogFile(Settings.output_dir() + '/' + 'err.log')
#for index, row in df.iterrows():
#    print('\r{0} of {1} processed'.format(index, df.shape[0]), end='')
#    
#    filename = (Settings.root_dir() + '/' +
#                str(row['file_date'].year) + '/' + str(row['file_date'].month).zfill(2) + '/'+
#                str(row['cik']).zfill(10) + '-' + row['adsh'] + '.zip'
#                )
#    dei = read_dei_section(filename, err)
#    dei['cik'] = row['cik']
#    dei['adsh'] = row['adsh']
#    data.append(dei)
#    
#rdf = pd.DataFrame(data)
#err.close()



#con = None
#try:
#    con = do.OpenConnection()
#    cur = con.cursor(dictionary=True)
#    cur.execute('select adsh, cik, period, period_end, fin_year from reports')
#    reps = pd.DataFrame(cur.fetchall())
#    
#except:
#    raise
#finally:
#    if con: con.close()
#    
#    
#m = pd.merge(df, reps, left_on='adsh', right_on='adsh')

#log = LogFile("outputs/log.txt", append=False)
#err = LogFile("outputs/err.txt", append=False)
#warn = LogFile("outputs/warn.txt", append=False)
#r = x.XBRLFile(log, warn, err)
#
#files = ['d:/sec/2018/02/0001043604-0001043604-18-000011.zip', #contextRef not in fact attrib
#        'd:/sec/2013/03/0001449488-0001449488-13-000018.zip', #role_uri not in chapters
#        'd:/sec/2014/08/0000754811-0000754811-14-000086.zip', #labels
#        'd:/sec/2015/01/0000317889-0001437749-15-000640.zip', #cicles in structure
#        'd:/sec/2013/03/0000886136-0001144204-13-015502.zip', #cicles in structure
#        'd:/sec/2016/02/0000317889-0001437749-16-026192.zip', #cicles in structure
#        'd:/sec/2016/03/0000317889-0001437749-16-026192.zip', #cicles in structure
#        'd:/sec/2016/06/0000890491-0001193125-16-630221.zip', #cicles in structure
#        'd:/sec/2017/04/0001541354-0001493152-17-004516.zip', #cicles in structure
#        'd:/sec/2013/03/0001393066-0001193125-13-087936.zip', #is problem
#        'd:/sec/2013/06/0000014693-0000014693-13-000038.zip', #lab problem
#        'd:/sec/2018/02/0000076605-0000076605-18-000045.zip' #bs:null
#        ]
#
#for file in files[0:1]:
#    r.read('z'+file[1:], None)
#    print(len(r.chapters), r.facts_df.shape, r.lab.shape, r.cntx)
#
#log.close()
#err.close()
#warn.close()
#
#"""
#******************************************
#"""
#
#chaps = []
#for _, c in r.chapters.items():
#    if c.chapter == 'sta':
#        chaps.append((c.label, c))