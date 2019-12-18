# -*- coding: utf-8 -*-

import datetime as dt
import json
import os
import re
from abc import ABCMeta, abstractmethod
from typing import Iterator, List, Set, TextIO, Tuple, Union, cast

import lxml

import utils
from mysqlio.basicio import OpenConnection
from algos.xbrljson import ForDBJsonEncoder, custom_decoder
from settings import Settings
from utils import add_root_dir, remove_root_dir


class FileRecord(object):
    def __init__(self):
        self.company_name: str = ""
        self.form_type: str = ""
        self.cik: int = 0
        self.sic: int = 0
        self.adsh: str = ""
        self.period: Optional[dt.date] = None
        self.file_date: Optional[dt.date] = None
        self.fye: str = ""
        self.fy: int = 0

    def __str__(self) -> str:        
        return json.dumps(self.__dict__, cls=ForDBJsonEncoder)

    def aslist(self) -> List[Union[str, int, dt.date, None]]:
        return list(self.__dict__.values())
    
def record_from_xbrl(elem: lxml.etree.Element) -> FileRecord:
    record = FileRecord()

    for e in elem.iter():
        name = re.sub('{.*}', '', e.tag.lower())
        if name == 'companyname':
            record.company_name = e.text.strip()
        if name == 'formtype':
            record.form_type = e.text.strip()
        if name == 'ciknumber':
            record.cik = int(e.text)
        if name == 'accessionnumber':
            record.adsh = e.text.strip()
        if name == 'period':
            text = e.text.strip()
            record.period = utils.str2date(text)
        if name == 'fiscalyearend':
            record.fye = e.text.strip()
        if name == 'filingdate':
            text = e.text.strip()
            record.file_date = utils.str2date(text, 'mdy')
        if name == 'assignedsic':
            record.sic = int(e.text.strip())
            
    if record.period is not None:
        record.fy = (record.period - dt.timedelta(days=365/2)).year

    return record

def record_from_str(data: str) -> FileRecord:
    record = FileRecord()
    record_data = json.loads(data, object_hook=custom_decoder)
    for k, v in record_data.items():
        if hasattr(record, k):
            record.__dict__[k] = v
    if record.period is not None:
        record.period = utils.str2date(record.period, 'ymd')
        if record.fy is None:
            record.fy = (record.period - dt.timedelta(days=365/2)).year
    if record.file_date is not None:
        record.file_date = utils.str2date(record.file_date, 'ymd')

    return record

class FilingRSS(object):
    def __init__(self):
        self.tree = None        
        return
    
    def open_file(self, filename: str) -> None:
        FilingRSS.__init__(self)

        try:
            self.tree = lxml.etree.parse(filename).getroot()
        except:
            self.tree = None        
    
    def filing_records(self) -> Iterator[FileRecord]:
        if self.tree is None:
            return
        
        for item in self.tree.findall(".//item"):
            yield record_from_xbrl(item)



def records_to_file(records: List[Tuple[FileRecord, str]], 
                    filename: str) -> None:
    with open(filename, 'w') as file:
        for (record, zip_filename) in records:
            file.write(str(record))
            file.write('\t')
            file.write(zip_filename)
            file.write('\n')

class RecordsEnumerator(metaclass=ABCMeta):
    @abstractmethod    
    def filing_records(self,
            all_types: bool=False,
            form_types: Set[str]={'10-K', '10-K/A'}) -> List[Tuple[FileRecord, str]]:
        pass
    
    
    
class XBRLEnumerator(RecordsEnumerator):
    def __init__(self, years: List[int], months: List[int]):
        self.years: List[int] = []
        self.months: List[int] = []
        self.setperiod(years, months)
        
    def setperiod(self, years: List[int], months: List[int]):
        self.years = years.copy()
        self.months = months.copy()
        
    def filing_records(self,
                       all_types: bool=False,
                       form_types: Set[str]={'10-K', '10-K/A'}) -> List[Tuple[FileRecord, str]]:
        records: List[Tuple[FileRecord, str]] = []

        rss = FilingRSS()
        adshs: Set[str] = set()
        for y in self.years:
            for m in self.months:
                year_month_dir = utils.year_month_dir(y, m)
                rssfilename = os.path.join(
                                    year_month_dir,
                                    'rss-{0}-{1}.xml'.format(y,str(m).zfill(2)))

                rss.open_file(rssfilename)
                for record in rss.filing_records():
                    if record.adsh in adshs:
                        continue
                    if not (all_types or record.form_type in form_types):
                        continue

                    adshs.add(record.adsh)
                    zip_filename = utils.posix_join(
                                        year_month_dir,
                                        '{0}-{1}.zip'.format(
                                            str(record.cik).zfill(10), 
                                            record.adsh))                    
                    
                    records.append((record, zip_filename))
        return records
                    
class CustomEnumerator(RecordsEnumerator):
    def __init__(self, filename: str):
        assert os.path.exists(filename)
        self.__filename = filename
    
    def filing_records(self, 
                       all_types: bool=False,
                       form_types: Set[str]={'10-K', '10-K/A'}) -> List[Tuple[FileRecord, str]]:
        records = []
        adshs: Set[str] = set()
        with open(self.__filename) as f:
            for line in f.readlines():
                jstr, filename = re.sub('\n*', '', line).split('\t')
                filename = add_root_dir(filename)
                record = record_from_str(jstr)

                if record.adsh in adshs:
                    continue
                if not (all_types or record.form_type in form_types):
                    continue

                adshs.add(record.adsh)                
                records.append((record, filename))
                
        return records

class MySQLEnumerator(RecordsEnumerator):
    def __init__(self):        
        self.query: str = ''
        self.after: dt.date = dt.date.today() - dt.timedelta(days=365)
        self.set_filter_method('all', self.after)

    def filing_records(self, 
                       all_types: bool=False,
                       form_types: Set[str]={'10-K', '10-K/A'}) -> List[Tuple[FileRecord, str]]:
        records: List[Tuple[FileRecord, str]] = []
        with OpenConnection() as con:
            cur = con.cursor(dictionary=True)
            cur.execute(self.query, {'after': self.after})
            for row in cur.fetchall():
                record = record_from_str(row['record'])
                if not (all_types or record.form_type in form_types):
                    continue
                records.append((record, add_root_dir(row['file_link'])))
        return records

    def set_filter_method(self, method: str, after: dt.date) -> None:
        assert method in {'new', 'bad', 'all'}
        if method == 'all':
            self.query = 'select * from sec_xbrl_forms where filed>=%(after)s'
        elif method == 'new':
            self.query = """select f.* from sec_xbrl_forms f
                            left outer join reports r
                                on r.adsh = f.adsh
                            left outer join 
                            (
                                select state as adsh from logs_parse
                                where levelname='error'
                                group by state
                            ) l
                                on l.adsh = f.adsh
                            where f.filed>=%(after)s
                                and r.adsh is null
                                and l.adsh is null;"""
        elif method == 'bad':
            self.query = """select f.* from sec_xbrl_forms f
                            left outer join reports r
                                on r.adsh = f.adsh
                            left outer join 
                            (
                                select state as adsh from logs_parse
                                where levelname='error'
                                group by state
                            ) l
                                on l.adsh = f.adsh
                            where f.filed>=%(after)s
                                and r.adsh is null
                                and l.adsh is not null;"""
        self.after = after

if __name__ == "__main__":
    pass
