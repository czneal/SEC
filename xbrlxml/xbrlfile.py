# -*- coding: utf-8 -*-

import pandas as pd
from collections import namedtuple

import xbrlxml.xbrlfileparser as xbrlfp
from xbrlxml.xsdfile import XSDFile
from xbrlxml.xbrlchapter import ReferenceParser
from xbrlxml.xbrlexceptions import XbrlException
import utils

XbrlFiles = namedtuple('XbrlFiles', ['xbrl', 'xsd', 'pres', 'defi', 'calc'])

class XbrlFile(object) :
    def __init__(self):
        self.schemes = {'calc': {},
                        'defi': {},
                        'pres': None,
                        'xsd': None}
        self.dei = None
        self.period = None
        self.fy = None
        self.fye = None
        self.contexts = None
        self.root = None
        
        self.numfacts = {'facts': None,
                         'units': None,
                         'footnotes': None
                         }
        self.dfacts = None
        
        self.text_blocks = None
        
        self.errlog = []
        self.warnlog = []
    
    def prepare(self, files, record):
        """Read calculation, presentation, definition and xsd schemes
        read dei and contexts section of xbrl file
        
        setup schemes property
        setup contexts property
        setup dei property
        setup period property
        setup root property
        
        if failed raise XbrlException()
        """
        self.__init__()        
        
        #read xbrl file into xmltree object        
        root = utils.opensmallxmlfile(files.xbrl)
        if root is None:
            root = utils.openbigxmlfile(files.xbrl)
        if root is None:
            raise XbrlException('unable open xbrl file') 
            
        #read data and setup properties
        self.root = root    
        self.__read_scheme_files(files)
        self.__read_dei_contexts()
        self.__find_period(record)        
        
    def __read_dei_contexts(self):
        """Parse dei section and contexts section of xbrl file
        setup dei property
        setup contexts property
        """
        parser = xbrlfp.XbrlParser()
        self.contexts = parser.parse_contexts(self.root)
        units = parser.parse_units(self.root)
        self.dei = parser.parse_dei(self.root, units)        
    
    def __read_scheme_files(self, files):
        """Read scheme files
        setup shemes property
        
        if failed raise XbrlException()
        """
        "unittested"
        try:
            rparser = ReferenceParser('presentation')
            self.schemes['pres'] = rparser.parse(files.pres)            
        except:
            raise XbrlException(msg = "couldn't read xbrl report without presentation scheme")
        try:
            self.schemes['xsd'] = XSDFile().read(files.xsd)
        except:
            raise XbrlException(msg = "couldn't read xbrl report without xsd scheme")
            
        try:
            rparser.setreftype('definition')
            self.schemes['defi'] = rparser.parse(files.defi)
        except:
            self.__logwarning('missing definition scheme')
            pass
        try:
            rparser.setreftype('calculation')
            self.schemes['calc'] = rparser.parse(files.calc)
        except:
            self.__logwarning('missing calculation scheme')
            pass
        
    def __find_period(self, record):
        """Find period date
        setup period property
        setup fy propery
        setup fye property
        
        if failed raise XbrlException()
        """
        period = None
        period_dei, period_rss = None, None
        
        if 'period' in self.dei and 'period' in record:
            data = [(p, context, len(self.contexts[context].dim)) for [p, context] in self.dei['period']]
            data = sorted(data, key=lambda x: x[2])
            
            period_dei = utils.str2date(data[0][0])
            period_rss = utils.str2date(record['period'])
            if period_dei == period_rss:
                period = period_dei
        if period is None:
            raise XbrlException('period in dei {0} doesnt match period in SEC rss {1}'.format(
                    period_dei, period_rss))
        
        self.period = period
        self.fy, self.fye = utils.calculate_fy_fye(period)            

    def read_units_facts_fn(self):
        """
        parse units, facts, footnotes
        make them clean
        setup numfacts property
        setup dfacts prperty
        """
        parser = xbrlfp.XbrlParser()
        units = parser.parse_units(self.root)
        footnotes = parser.parse_footnotes(self.root)
        facts = parser.parse_facts(self.root)
                        
        facts = xbrlfp.xbrltrans.transform_facts(facts)
        footnotes = xbrlfp.xbrltrans.transform_fn(facts, footnotes)
        
        xclean = xbrlfp.XbrlCleanUp()
        (self.numfacts['facts'], 
         self.numfacts['units'], 
         _, 
         self.numfacts['footnotes']) = xclean.cleanup(
                 facts, units, self.contexts, footnotes)
        
        #transform facts and contexts into DataFrame object
        dfacts = pd.DataFrame([f.asdict() for f in facts.values()])
        dcontexts = pd.DataFrame([c.asdict() 
                                    for c in self.contexts.values()])    
        dcontexts = dcontexts[dcontexts['edate'] == self.period]
        dunits = pd.DataFrame(
                     [[unitid, str(u)] for unitid, u in units.items()],
                     columns=['unitid', 'uom'])
        self.dfacts = (dfacts.merge(dcontexts, 
                                   left_on='context', 
                                   right_on='context')
                            .merge(dunits,
                                   left_on='unitid',
                                   right_on='unitid'))
        
            
    def read_text_blocks(self, text_blocks) -> None:
        parser = xbrlfp.XbrlParser()
        self.text_blocks = parser.parse_textblocks(self.root, text_blocks)
    
    def __logerror(self, message):
        self.errlog.append(message)
    
    def __logwarning(self, message):
        self.warnlog.append(message)
    
    
    