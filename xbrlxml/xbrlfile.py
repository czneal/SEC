# -*- coding: utf-8 -*-

import pandas as pd #type: ignore
import datetime as dt
from collections import namedtuple
from typing import Dict, Optional

import utils
import logs
import xbrlxml.xbrlfileparser as xbrlfp
import xbrlxml.truevalues as tv
from xbrlxml.xsdfile import XSDFile
from xbrlxml.xbrlchapter import ReferenceParser, CalcChapter, Chapter
from xbrlxml.xbrlexceptions import XbrlException


XbrlFiles = namedtuple('XbrlFiles', ['xbrl', 'xsd', 'pres', 'defi', 'calc'])

class XbrlFile(object) :
    def __init__(self):
        self.schemes = {}
        self.schemes['calc']: Dict[str, CalcChapter] = {}
        self.schemes['defi']: Dict[str, Chapter] = {}
        self.schemes['pres'] = None,
        self.schemes['xsd'] = None

        self.dei = None
        self.period: Optional[dt.date] = None
        self.fy: Optional[int] = None
        self.fye: Optional[str] = None
        self.contexts = None
        self.root = None
        self.company_name: Optional[str] = None
        self.adsh: Optional[str] = None
        
        self.numfacts = {'facts': None,
                         'units': None,
                         'footnotes': None
                         }
        self.dfacts = None
        
        self.text_blocks = None
    
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
        self.company_name = record['company_name']
        self.adsh = record['adsh']
        
        #read xbrl file into xmltree object        
        root = utils.opensmallxmlfile(files.xbrl)
        if root is None:
            root = utils.openbigxmlfile(files.xbrl)
        if root is None:
            msg = 'unable open xbrl file'
            logs.get_logger(__name__).error(msg=msg)
            raise XbrlException(msg) 
        
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
        
        logger = logs.get_logger(__name__)
        try:
            rparser = ReferenceParser('presentation')
            self.schemes['pres'] = rparser.parse(files.pres)            
        except:
            msg = "couldn't read xbrl report without presentation scheme"
            logger.error(msg=msg)
            raise XbrlException(msg=msg)
        try:
            self.schemes['xsd'] = XSDFile().read(files.xsd)
        except:
            msg = "couldn't read xbrl report without xsd scheme"
            logger.error(msg=msg)
            raise XbrlException()
            
        try:
            rparser.setreftype('definition')
            self.schemes['defi'] = rparser.parse(files.defi)
        except:
            logger.warning('missing definition scheme')
            pass
        try:
            rparser.setreftype('calculation')
            self.schemes['calc'] = rparser.parse(files.calc)
        except:
            logger.warning('missing calculation scheme')
            pass
        
    def __find_period(self, record):
        """Find period date
        setup period property
        setup fy propery
        setup fye property
        
        if failed raise XbrlException()
        """
        
        period = tv.TRUE_VALUES.get_true_period(self.adsh)
        period_dei, period_rss = None, None
        
        if (period is None and 
                'period' in self.dei and 
                'period' in record):
            data = [(p, context, len(self.contexts[context].dim)) for [p, context] in self.dei['period']]
            data = sorted(data, key=lambda x: x[2])
            
            period_dei = utils.str2date(data[0][0])
            period_rss = utils.str2date(record['period'])
            if period_dei == period_rss:
                period = period_dei
        if period is None:
            msg = 'period match failed'
            extra = {'period_dei': period_dei,
                     'period_rss': period_rss}
            logs.get_logger(__name__).error(msg=msg, extra=extra)
            raise XbrlException(msg)
        
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
        (facts, units, _, footnotes) = xclean.cleanup(
                 facts, units, self.contexts, footnotes)
        self.numfacts['facts'] = facts
        self.numfacts['units'] = units
        self.numfacts['footnotes'] = footnotes
        
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
    
    def any_gaap_fact(self):
        if self.dfacts is None:
            return False
        return 'us-gaap' in self.dfacts['version'].unique()
    
    
    