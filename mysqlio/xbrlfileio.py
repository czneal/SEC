# -*- coding: utf-8 -*-
"""
Created on Wed Jul 31 18:28:09 2019

@author: Asus
"""
import json
from mysql.connector.errors import InternalError, Error

import mysqlio.basicio
import queries as q
import algos.xbrljson
from xbrlxml.xbrlexceptions import XbrlException
from xbrlxml.dataminer import SharesDataMiner
from log_file import Logs, RepeatFile
from utils import remove_root_dir

class ReportToDB(object):
    def __init__(self, logs: Logs, repeat: RepeatFile):
        self.__logs = logs
        self.__repeat = repeat
        
        with mysqlio.basicio.OpenConnection() as con:
            self.reports = mysqlio.basicio.Table('reports', con, buffer_size=1)
            self.companies = mysqlio.basicio.Table('companies', con, buffer_size=1)
            self.nums = mysqlio.basicio.Table('mgnums', con, buffer_size=1000)
            self.shares = mysqlio.basicio.Table('raw_shares', con, buffer_size=1000)
            
    def _dumps_structure(self, miner):
        structure = {}
        for sheet, roleuri in miner.sheets.mschapters.items():
            xsd_chapter = miner.xbrlfile.schemes['xsd'].get(roleuri, None)
            calc_chapter = (miner.xbrlfile
                            .schemes['calc']
                            .get(roleuri, {"roleuri": roleuri}))
            structure[sheet] = {
                    'label': xsd_chapter.label,
                    'chapter': calc_chapter
                    }
            
        return json.dumps(structure, cls=algos.xbrljson.ForDBJsonEncoder)
    
    def _dums_contexts(self, miner):
        return json.dumps(miner.extentions)
    
    def write_report(self, cur, record, miner):
        file_link = remove_root_dir(miner.zip_filename)
        report = {'adsh': miner.adsh,
                  'cik': miner.cik,
                  'period': miner.xbrlfile.period,
                  'period_end': miner.xbrlfile.fye,
                  'fin_year': miner.xbrlfile.fy,
                  'taxonomy': miner.xbrlfile.dei['us-gaap'],
                  'form': record['form_type'],
                  'quarter': 0,
                  'file_date': record['file_date'],
                  'file_link': file_link,
                  'trusted': 1,
                  'structure': self._dumps_structure(miner),
                  'contexts': self._dums_contexts(miner)                  
                }
        if not self.reports.write(report, cur):
            raise XbrlException('couldnt write to mysql.reports table')
        
    
    def write_nums(self, cur, record, miner):
        if miner.numeric_facts is None:
            return
        
        if not self.nums.write_df(df=miner.numeric_facts,
                                  cur=cur):
            raise XbrlException('couldnt write to mysql.mgnums table')
    
    @staticmethod
    def write_company(cur, record):
        company = {'company_name': record['company_name'],
                   'sic': record['sic'] if record['sic'] is not None else 0,
                   'cik': record['cik'],
                   'updated': record['file_date']}
        insert = q.insert_update_companies
        try:
            mysqlio.basicio.tryout(5, InternalError,
                                   cur.execute, insert, company)
        except InternalError as err:
            raise err
        except Error:
            raise XbrlException('couldnt write to mysql.companies table')
            
    def write(self, cur, record, miner):
        dead_lock_trys = 0
        while(True):
            try:
                ReportToDB.write_company(cur, record)
                self.write_report(cur, record, miner)
                self.write_nums(cur, record, miner)
                break               
                
            except InternalError as e:
                #if dead lock just repeat again
                if dead_lock_trys < 100:
                    dead_lock_trys += 1
                    continue
                else:
                    self.__logs.error(str(e))
                    self.__logs.error('mysql super dead lock')
                    self.__repeat.repeat()
                    break
            except XbrlException as e:
                self.__logs.error(str(e))
                self.__logs.error('problems with writing into mysql database')
                self.__repeat.record()
                break
            except:
                self.__logs.traceback()
                self.__logs.error('unexpected problems with writing into mysql database')
                self.__repeat.repeat()
                break
            
    def write_shares(self, cur, record, miner: SharesDataMiner) -> None:
        if miner.shares_facts.shape[0] == 0:
            return
        
        self.shares.write_df(miner.shares_facts, cur)
    
    def flush(self, cur):
        self.reports.flush(cur)
        self.companies.flush(cur)
        self.nums.flush(cur)