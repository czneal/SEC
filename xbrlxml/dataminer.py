# -*- coding: utf-8 -*-

import pandas as pd
from abc import ABCMeta, abstractmethod

from xbrlxml.xbrlfile import XbrlFile
from xbrlxml.selectors import ChapterChooser, ContextChooser, ChapterExtender
from xbrlxml.xbrlzip import XBRLZipPacket
from xbrlxml.xbrlexceptions import XbrlException
from log_file import Logs, RepeatFile

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
    def __init__(self, logs: Logs, repeat: RepeatFile):
        self.xbrlfile = XbrlFile()
        self.xbrlzip = XBRLZipPacket()
        self.sheets = ChapterChooser(self.xbrlfile)
        self.cntx = ContextChooser(self.xbrlfile)
        self.extender = ChapterExtender(self.xbrlfile)
        
        self._logs = logs
        self._repeat = repeat
        
        self.cik = None
        self.adsh = None
        self.zip_filename = None
        self.extentions = []
        self.numeric_facts = None
    
    def _choose_main_sheets(self):
        self.sheets.choose()
        for sheet in ['is', 'bs', 'cf']:
            if sheet not in self.sheets.mschapters:
                self._logs.warning('"{0}" not found'.format(sheet))
    
    def _extend_calc(self):        
        for roleuri in self.sheets.mschapters.values():
            warnings = self.extender.find_extentions(roleuri)
            [self._logs.warning(w) for w in warnings]
            self.extender.extend()
            self.extentions.extend(self.extender.extentions)
            
    def _find_main_sheet_contexts(self):
        for sheet, roleuri in self.sheets.mschapters.items():
            context = self.cntx.choose(roleuri)
            if context is None:
                self._logs.warning('for "{0}": {1} context not found'.format(
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
            self._logs.error(e)
            return False
        
        try:
            good = False            
            self.xbrlfile.prepare(self.xbrlzip, record)
            self.do_job()            
            good = True
            
            self._logs.log('has been read')            
        except XbrlException as e:
            self._logs.error(e)                    
        except:
            self._logs.traceback()
        finally:
            for line in self.xbrlfile.errlog:
                self._logs.error(line)
            for line in self.xbrlfile.warnlog:
                self._logs.warning(line)
            if not good:          
                self._repeat.repeat()
        return good
        
class NumericDataMiner(DataMiner):
    def do_job(self):
        self.xbrlfile.read_units_facts_fn()
        
        DataMiner._choose_main_sheets(self)        
        DataMiner._find_main_sheet_contexts(self)
        DataMiner._extend_calc(self)
        DataMiner._gather_numeric_facts(self)
        
        #self.numeric_facts.to_csv('outputs/numeric_facts.csv')
