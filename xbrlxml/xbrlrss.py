# -*- coding: utf-8 -*-

import lxml
import datetime as dt
import re
import json
import os

import utils
from algos.xbrljson import ForDBJsonEncoder
from settings import Settings

class FilingRSS(object):
    def __init__(self):
        self.tree = None        
        return
    
    def open_file(self, filename):
        self.__init__()        
        try:
            self.tree = lxml.etree.parse(filename).getroot()
        except:
            self.tree = None        
    
    def filing_records(self):
        if self.tree is None:
            return []
        
        root = self.tree
        records = []
        for item in root.findall(".//item"):
            r = FilingRecord()
            r.read(item)
            if r.form_type in {'10-K','10-K/A'}:
                records.append(r.asdict())
                
        return records
    
    
class FilingRecord(object):
    __attribs = ['company_name', 'form_type', 'cik', 'sic', 'adsh', 'period', 'file_date', 'fye', 'fy']
    def __init__(self):
        self.company_name = None
        self.form_type = None
        self.cik = None
        self.sic = None
        self.adsh = None
        self.period = None
        self.file_date = None
        self.fye = None
        self.fy = None
        
    def read(self, record):
        ""
        self.__init__()
        for e in record.iter():
            name = re.sub('{.*}', '', e.tag.lower())
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
                self.period = utils.str2date(text)
            if name == 'fiscalyearend':
                self.fye = e.text.strip()
            if name == 'filingdate':
                text = e.text.strip()
                self.file_date = utils.str2date(text, 'mdy')
            if name == 'assignedsic':
                self.sic = int(e.text.strip())
                
        if self.period is not None:
            self.fy = (self.period - dt.timedelta(days=365/2)).year
        return
    
    def __str__(self):
        return self.aslist().__str__()
    
    def aslist(self):
        return [getattr(self, a) for a in self.__attribs]
    
    def asdict(self):
        return {a:getattr(self, a) for a in self.__attribs}
    
class RecordsEnumerator(object):
    def filing_records(self):
        pass
    
    
class SecEnumerator(RecordsEnumerator):
    def __init__(self, years, months):
        self.years = []
        self.months = []
        self.setperiod(years, months)
        
    def setperiod(self, years, months):
        self.years = [y for y in years]
        self.months = [m for m in months]
        
    def filing_records(self):
        rss = FilingRSS()
        for y in self.years:
            for m in self.months:
                filename = 'rss-{0}-{1}.xml'.format(y,str(m).zfill(2))
                filedir = '{0}{1}/{2}/'.format(
                            Settings.root_dir(), 
                            str(y).zfill(4), str(m).zfill(2))
                rss.open_file(filedir + filename)
                        
                for record in rss.filing_records():
                    if record['form_type'] not in {'10-K', '10-K/A'}:
                        continue
                    yield record, '{0}{1}-{2}.zip'.format(filedir,
                                   str(record['cik']).zfill(10), 
                                   record['adsh'])
                    
class CustomEnumerator(RecordsEnumerator):
    def __init__(self, filename):
        assert os.path.exists(filename)
        self.__filename = filename
    
    def filing_records(self):
        records = []
        with open(self.__filename) as f:
            for line in f.readlines():
                jstr, filename = re.sub('\n*', '', line).split('\t')
                record = json.loads(jstr)
                record['file_date'] = utils.str2date(record['file_date'])
                record['period'] = utils.str2date(record['period'])
                records.append([record, filename])
                
        return records

def makecustomrss() :
    with open('../outputs/customrss.csv', 'w') as f:
        rss = SecEnumerator([2019], months=[m for m in range(1,13)])
        for (record, filename) in rss.filing_records():            
            f.write(json.dumps(record, cls=ForDBJsonEncoder))
            f.write('\t' + filename + '\n')
            
if __name__ == "__main__":
    makecustomrss()
    