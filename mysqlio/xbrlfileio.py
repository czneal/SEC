# -*- coding: utf-8 -*-
"""
Created on Wed Jul 31 18:28:09 2019

@author: Asus
"""
import json

import mysqlio.basicio
import algos.xbrljson
from xbrlxml.xbrlexceptions import XbrlException

class ReportToDB(object):
    def __init__(self):        
        with mysqlio.basicio.OpenConnection() as con:
            self.reports = mysqlio.basicio.Table('reports', con, buffer_size=1)
            self.companies = mysqlio.basicio.Table('companies', con, buffer_size=1)
            self.nums = mysqlio.basicio.Table('mgnums', con, buffer_size=1000)
    def _dumps_structure(self, miner):
        structure = {}
        for sheet, roleuri in miner.sheets.mschapters.items():
            xsd_chapter = miner.xbrlfile.schemes['xsd'].get(roleuri, None)
            calc_chapter = miner.xbrlfile.schemes['calc'].get(roleuri, {})            
            structure[sheet] = {
                    'label': xsd_chapter.label,
                    'chapter': calc_chapter
                    }
            
        return json.dumps(structure, cls=algos.xbrljson.ForDBJsonEncoder)
    
    def _dums_contexts(self, miner):
        return json.dumps(miner.extentions)
    
    def write_report(self, cur, record, miner):        
        report = {'adsh': miner.adsh,
                  'cik': miner.cik,
                  'period': miner.xbrlfile.period,
                  'period_end': miner.xbrlfile.fye,
                  'fin_year': miner.xbrlfile.fy,
                  'form': record['form_type'],
                  'quarter': 0,
                  'file_date': record['file_date'],
                  'file_link': miner.zip_filename,
                  'trusted': 1,
                  'structure': self._dumps_structure(miner),
                  'contexts': self._dums_contexts(miner)
                }
        if not self.reports.write(report, cur):
            raise XbrlException('couldnt write to mysql.reports table')
        
    
    def write_nums(self, cur, record, miner):
        if not self.nums.write_df(df=miner.numeric_facts,
                                  cur=cur):
            raise XbrlException('couldnt write to mysql.mgnums table')
    
    def write_company(self, cur, record):
        company = {'company_name': record['company_name'],
                   'sic': record['sic'],
                   'cik': record['cik']
                    }
        if not self.companies.write(company, cur):
            raise XbrlException('couldnt write to mysql.companies table')
            
    def write(self, cur, record, miner):
        try:
            self.write_company(cur, record)
            self.write_report(cur, record, miner)
            self.write_nums(cur, record, miner)
        except XbrlException as e:
            miner.error(str(e))
        except:
            miner.traceback()
    
    def flush(self, cur):
        self.reports.flush(cur)
        self.companies.flush(cur)
        self.nums.flush(cur)