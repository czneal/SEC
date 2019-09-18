# -*- coding: utf-8 -*-

import pandas as pd
from abc import ABCMeta, abstractmethod
from typing import List, Dict, Set

from classifiers.mainsheets import MainSheets

from algos.scheme import enum
from xbrlxml.xbrlfile import XbrlFile
from xbrlxml.selectors import ChapterChooser, ContextChooser, ChapterExtender
from xbrlxml.selectors import SharesChapterChooser
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
        
        self.shares = SharesChapterChooser(self.xbrlfile)
        self.shares_contexts: Dict[str, str] = {}
        
        self._logs = logs
        self._repeat = repeat
        
        self.cik = None
        self.adsh = None
        self.zip_filename = None
        self.extentions: List[Dict[str, str]] = []
        self.numeric_facts = None
        self.shares_facts = None
    
    def _choose_main_sheets(self):
        self.sheets.choose()
        if not self.sheets.mschapters:
            self._logs.warning('couldnt find any main sheet')
        else:
            for sheet in ['is', 'bs', 'cf']:
                if sheet not in self.sheets.mschapters:
                    self._logs.warning('"{0}" chapter not found'.format(sheet))
    
    def _choose_shares_sheets(self):
        self.shares.choose()
        if not self.shares.share_chapters:
            self._logs.warning('couldnt find any shares sheet')
        if len(self.shares.share_chapters) > 1:
            self._logs.warning('too many shares sheets')                    
    
    def _extend_calc(self):        
        for roleuri in self.sheets.mschapters.values():
            warnings = self.extender.find_extentions(roleuri)
            [self._logs.warning(w) for w in warnings]
            self.extender.extend()
            self.extentions.extend(self.extender.extentions)
            
    def _find_main_sheet_contexts(self):
        self.extentions = []
        
        for sheet, roleuri in self.sheets.mschapters.items():
            context = self.cntx.choose(roleuri)
            if context is None:
                self._logs.warning(
                        'for "{0}": {1} context not found'.format(
                                        sheet, roleuri))
            else:
                self.extentions.append({'roleuri': roleuri,
                                        'context': context})
    
    def _find_shares_sheet_contexts(self):
        self.shares_contexts: Dict[str, str] = {}
        
        for roleuri in self.shares.share_chapters:
            context = self.cntx.choose(roleuri)
            if context is None:
                self._logs.warning('for shares chapter "{0}" context not found'
                                  .format(roleuri))
            else:
                self.shares_contexts[roleuri] = context
        
    def _gather_numeric_facts(self):
        frames = []
        for e in self.extentions:
            pres = self.xbrlfile.schemes['pres'].get(e['roleuri'])
            calc = self.xbrlfile.schemes['calc'].get(e['roleuri'], pres)
            if 'label' in e:
                tags = set()
                if e['label'] in pres.nodes:
                    tags.update([e for [e] in 
                                     enum(structure=pres.nodes[e['label']], 
                                          outpattern='c')])
                if e['label'] in calc.nodes:
                    tags.update([e for [e] in 
                                     enum(structure=calc.nodes[e['label']], 
                                          outpattern='c')])
            else:
                tags = set(pres.gettags()).union(set(calc.gettags()))
            
            frame = self.xbrlfile.dfacts
            frame = frame[(frame['name'].isin(tags)) & 
                          (frame['context'] == e['context'])]
            frames.append(frame)
        
        if not frames:
            if self.sheets.mschapters and self.xbrlfile.any_gaap_fact():
                raise XbrlException('couldnt find any facts to write')
            else:
                self._logs.warning('couldnt find any us-gaap fact')
                self.numeric_facts = None
                return
            
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
        
        if self.numeric_facts.shape[0] == 0:
            self._logs.warning('only null facts found')
            self.numeric_facts = None
            
    def _gather_shares_facts(self):
#        frames = []
#        pres = self.xbrlfile.schemes['pres']
#        for roleuri, context in self.shares_contexts.items():
#            frame = self.xbrlfile.dfacts
#            indexer = ((frame['uom'].str.contains('shares')) &
#                       (frame['name'].isin(pres[roleuri].gettags())) &
#                       (frame['context'] == context))
#            frame = frame[indexer].copy()
#            frame['roleuri'] = roleuri
#            frames.append(frame)
#            
#        if not frames:
#            self._logs.warning('couldnt find any share facts')                                
#            return
#        
#        self.shares_facts = pd.concat(frames).dropna(subset=['value'])
#        
#        self.shares_facts['adsh'] = self.adsh
#        self.shares_facts['fy'] = self.xbrlfile.fy
#        self.shares_facts['type'] = (
#                self.shares_facts['sdate'].apply(
#                        lambda x: 'I' if pd.isna(x) else 'D')
#                )
#        self.shares_facts.rename(columns={'edate':'ddate'}, inplace=True)
        frame = self.xbrlfile.dfacts
        self.shares_facts = frame[frame['uom'].str.contains('shares')].copy()
        self.shares_facts.dropna(subset=['value'], inplace=True)
        self.shares_facts['adsh'] = self.adsh
        self.shares_facts['fy'] = self.xbrlfile.fy
        self.shares_facts['type'] = (
                self.shares_facts['sdate'].apply(
                        lambda x: 'I' if pd.isna(x) else 'D')
                )
        self.shares_facts.rename(columns={'edate':'ddate'}, inplace=True)
        self.shares_facts['member'] = None
        
        cntx = self.xbrlfile.contexts
        for context in list(self.shares_facts['context'].unique()):
            member = ','.join([d['member'] 
                                    for d in cntx[context].asdictdim()
                                        if d['member'] is not None])
            self.shares_facts.loc[
                    self.shares_facts['context'] == context, 
                    'member'
                    ] = member
        
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
            good = False
            self.xbrlzip.open_packet(zip_filename)
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
        
class SharesDataMiner(DataMiner):    
    def do_job(self):
        self.xbrlfile.read_units_facts_fn()
        
        self._choose_shares_sheets()
        self._find_shares_sheet_contexts()
        self._gather_shares_facts()
        
class ChapterNamesMiner(DataMiner):
    def do_job(self):
        self.xbrlfile.read_units_facts_fn()
         
        self._choose_shares_sheets()
        self._gather_shares_facts()
        