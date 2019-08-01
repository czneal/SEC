# -*- coding: utf-8 -*-

import pandas as pd
import json
import sys
from abc import ABCMeta, abstractmethod

from xbrlxml.xbrlfile import XbrlFile
from xbrlxml.selectors import ChapterChooser, ContextChooser, ChapterExtender
from xbrlxml.xbrlzip import XBRLZipPacket
from xbrlxml.xbrlexceptions import XbrlException
from log_file import LogFile
from algos.xbrljson import ForDBJsonEncoder

class TextBlocks(object):
    def __init__(self):
        self.data = []
        
    def extend(self, blocks, record):
        self.data.extend(
                [{'adsh': record['adsh'],
                  'cik': record['cik'],
                  'name': block['name'],
                  'context': block['context'],
                  'text': block['value'].replace('&lt;', '<')
                                         .replace('&gt;', '>')
                                         .replace('&amp;', '&')}
                    for block in blocks]
            )
    
    def write(self, filename):
        df = pd.DataFrame(self.data)
        df.to_csv(filename)
        
class DataMiner(metaclass=ABCMeta):
    def __init__(self, log_dir, repeat_filename, append_log=False):
        self.xbrlfile = XbrlFile()
        self.xbrlzip = XBRLZipPacket()
        self.sheets = ChapterChooser(self.xbrlfile)
        self.cntx = ContextChooser(self.xbrlfile)
        self.extender = ChapterExtender(self.xbrlfile)
        
        self.__err = LogFile(log_dir + 'log.err', append=append_log)
        self.__warn = LogFile(log_dir + 'log.warn', append=append_log)
        self.__log = LogFile(log_dir + 'log.log', append=append_log)
        self.repeat = open(repeat_filename, 'w')
        
        self.cik = None
        self.adsh = None
        self.zip_filename = None
        self.extentions = []
        self.numeric_facts = None
    
    def _choose_main_sheets(self):
        self.sheets.choose()
        for sheet in ['is', 'bs', 'cf']:
            if sheet not in self.sheets.mschapters:
                self.warning('"{0}" not found'.format(sheet))
    
    def _extend_calc(self):        
        for roleuri in self.sheets.mschapters.values():
            warnings = self.extender.find_extentions(roleuri)
            [self.warning(w) for w in warnings]
            self.extender.extend()
            self.extentions.extend(self.extender.extentions)
            
    def _find_main_sheet_contexts(self):
        for sheet, roleuri in self.sheets.mschapters.items():
            context = self.cntx.choose(roleuri)
            if context is None:
                self.warning('for "{0}": {1} context not found'.format(
                        sheet, roleuri))
            self.extentions.append({'roleuri': roleuri,
                                    'context': context})
            
    def _gather_numeric_facts(self):
        frames = []
        for e in self.extentions:
            pres = self.xbrlfile.schemes['pres'].get(e['roleuri'])
            calc = self.xbrlfile.schemes['calc'].get(e['roleuri'], pres)
            tags = set(pres.gettags()).union(set(calc.gettags()))
            
            frame = self.xbrlfile.dfacts
            frame = frame[(frame['name'].isin(tags)) & 
                          (frame['context'] == e['context'])]
            frames.append(frame)
            
        self.numeric_facts = pd.concat(frames)
        self.numeric_facts = self.numeric_facts[
                pd.notna(self.numeric_facts['value'])]
        self.numeric_facts['adsh'] = self.adsh
        self.numeric_facts['fy'] = self.xbrlfile.fy
        self.numeric_facts['type'] = (
                self.numeric_facts['sdate'].apply(
                        lambda x: 'I' if pd.isna(x) else 'D')
                )
        self.numeric_facts.rename(columns={'edate':'ddate'}, inplace=True)
    
    def _read_text_blocks(self):
        raise XbrlException('_read_text_blocks is not implemented')
    
    def _prepare(self, record, zip_filename):
        self.extentions = []
        self.numeric_facts = None
        self.cik = record['cik']
        self.adsh = record['adsh']
        self.zip_filename = zip_filename
        pass
    
    @abstractmethod
    def do_job(self):
        pass
    
    def feed(self, record, zip_filename):
        self._prepare(record, zip_filename)
        
        try:
            self.xbrlzip.open_packet(zip_filename)
        except XbrlException as e:
            self.error(e)
            return
        
        try:
            good = False            
            self.xbrlfile.prepare(self.xbrlzip, record)
            self.do_job()            
            good = True
            
            self.log('has been read')            
        except XbrlException as e:
            self.error(e)                          
        except:
            self.traceback()
        finally:
            for line in self.xbrlfile.errlog:
                self.error(line)
            for line in self.xbrlfile.warnlog:
                self.warning(line)
            if not good:          
                self.repeat.write(json.dumps(
                                    record, 
                                    cls=ForDBJsonEncoder))
                self.repeat.write('\t' + self.zip_filename + '\n')
                
    def finish(self):
        self.repeat.close()
        self.__err.close()
        self.__log.close()
        self.__warn.close()
            
    def warning(self, message):
        self.__warn.writemany(self.cik, self.adsh, self.zip_filename, 
                          info = str(message))
    
    def error(self, message):
        self.__err.writemany(self.cik, self.adsh, self.zip_filename, 
                          info = str(message))
    def traceback(self):
        self.__err.writetb(self.cik, self.adsh, self.zip_filename, 
                        excinfo = sys.exc_info())
    
    def log(self, message):
        self.__log.writemany(self.cik, self.adsh, self.zip_filename, 
                          info = str(message))

class NumericDataMiner(DataMiner):
    def do_job(self):
        self.xbrlfile.read_units_facts_fn()
        
        DataMiner._choose_main_sheets(self)        
        DataMiner._find_main_sheet_contexts(self)
        DataMiner._extend_calc(self)
        DataMiner._gather_numeric_facts(self)
        
        #self.numeric_facts.to_csv('outputs/numeric_facts.csv')
