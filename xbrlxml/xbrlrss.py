# -*- coding: utf-8 -*-

import lxml
import datetime as dt
import re
import json
import os
from typing import List, Set
from abc import ABCMeta, abstractmethod

import utils
from algos.xbrljson import ForDBJsonEncoder
from settings import Settings
from utils import add_root_dir

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
    
    def filing_records(self, form_types: Set[str], all_types: bool=False):
        if self.tree is None:
            return []
        
        root = self.tree
        records = []
        for item in root.findall(".//item"):
            r = FilingRecord()
            r.read(item)
            if all_types or (r.form_type.upper() in form_types):
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
    
class RecordsEnumerator(metaclass=ABCMeta):
    @abstractmethod    
    def filing_records(self):
        pass
    
    def all_form_types(self) -> Set[str]:
        return set(['10-K', '10-K/A', '20-F', '10-Q', '10-Q/A'])
    
    
class SecEnumerator(RecordsEnumerator):
    def __init__(self, years, months):
        self.years = []
        self.months = []
        self.setperiod(years, months)
        
    def setperiod(self, years, months):
        self.years = [y for y in years]
        self.months = [m for m in months]
        
    def filing_records(self,
                       all_types: bool=False,
                       form_types: Set[str]={'10-K', '10-K/A'}):
        if all_types:
            form_types = self.all_form_types()
            
        rss = FilingRSS()
        for y in self.years:
            for m in self.months:
                filename = 'rss-{0}-{1}.xml'.format(y,str(m).zfill(2))
                filedir = '{0}{1}/{2}/'.format(
                            Settings.root_dir(), 
                            str(y).zfill(4), str(m).zfill(2))
                rss.open_file(filedir + filename)
                        
                for record in rss.filing_records(form_types=form_types,
                                                 all_types=all_types):
                    zip_filepath = '{0}-{1}'.format(
                                        str(record['cik']).zfill(10), 
                                        record['adsh'])
                    zip_filepath = os.path.join(filedir, zip_filepath)
                    zip_filepath = utils.remove_root_dir(zip_filepath)
                    yield (record, zip_filepath)
                    
class CustomEnumerator(RecordsEnumerator):
    def __init__(self, filename):
        assert os.path.exists(filename)
        self.__filename = filename
    
    def filing_records(self, 
                       all_types: bool=False,
                       form_types: Set[str]={'10-K', '10-K/A'}):
        if all_types:
            form_types = self.all_form_types()
            
        records = []
        with open(self.__filename) as f:
            for line in f.readlines():
                jstr, filename = re.sub('\n*', '', line).split('\t')
                filename = add_root_dir(filename)
                record = json.loads(jstr)
                record['file_date'] = utils.str2date(record['file_date'])
                record['period'] = utils.str2date(record['period'])
                if record['form_type'].upper() in form_types:
                    records.append([record, filename])
                
        return records

def makecustomrss(years: List[int], 
                  months: List[int], 
                  rss_name: str) -> None:
    filename = os.path.join(Settings.app_dir(), 
                            Settings.output_dir(), 
                            rss_name)
    with open(filename, 'w') as f:
        rss = SecEnumerator(years=years, months=months)
        for (record, filename) in rss.filing_records():            
            f.write(json.dumps(record, cls=ForDBJsonEncoder))
            f.write('\t' + filename + '\n')
            
if __name__ == "__main__":
    makecustomrss([2018], [1,2], 'onemonth.csv')
    