# -*- coding: utf-8 -*-
"""
Created on Wed May 29 12:18:19 2019

@author: Asus
"""

import re
from xbrlxml.xbrlexceptions import XbrlException
import utils

class Fact():
    default_decimals = 19
    default_value = None
    
    def __init__(self):
        self.tag = None
        self.version = None
        self.value = None
        self.context = None
        self.unitid = None
        self.decimals = None
        self.factid = None
    
    def decimalize(self):
        "unittested"
        if self.decimals is None or self.decimals == '':
            self.decimals = self.default_decimals
        else:
            try:
                self.decimals = abs(int(self.decimals))
            except ValueError:
                self.decimals = self.default_decimals
            
        if self.value is None or self.value == '':
            self.value = self.default_value
        else:
            try:
                self.value = float(self.value)
                if abs(self.value) >= 10**19:
                    self.value = 0
            except ValueError:
                self.value = self.default_value
            
    def update(self, fact):
        "unittested"
        fact.decimalize()
        self.decimalize()
        if (fact.value is None):
            return
        
        if self.decimals > fact.decimals:
            self.value = fact.value
            self.decimals = fact.decimals
    
    def name(self):
        "unittested"
        return '{0}:{1}'.format(self.version, self.tag)
    
    def key(self):
        "unittested"
        return (self.name(), self.context)
    
    def asdict(self):
        "unittested"
        return {'name': self.name(),
                'tag': self.tag,
                'version': self.version,
                'value': self.value,
                'context': self.context,
                'unitid': self.unitid,
                'decimals': self.decimals,
                'factid': self.factid}
        
    def __eq__(self, f):
        "unittested"
        if f.name() == self.name() and f.context == self.context:
            return True
        else:
            return False
    
class Context():
    def __init__(self):
        self.contextid = None
        self.sdate = None
        self.edate = None
        self.entity = None
        self.dim = [None]
        self.member = [None]
    
    def axises(self):
        return len(self.dim) - 1
    
    def isinstant(self):
        "unittested"
        if self.sdate is None:
            return True
        else:
            return False
        
    def asdictdim(self):
        "unittested"
        retval = []
        for d, m in zip(self.dim, self.member):
            retval.append({'context':self.contextid,
                           'sdate':self.sdate,
                           'edate':self.edate,
                           'dim': d,
                           'member': m})
        return retval
    
    def asdict(self):
        "unittested"
        return {'context':self.contextid,
                'sdate':self.sdate,
                'edate':self.edate}
        
    def isdimentional(self):
        "unittested"
        return (len(self.dim) > 1)
    
    def issuccessor(self):
        "unittested"
        for m in self.member:
            if m is None: continue
            if re.match('.*successor.*', m, flags=re.I):
                return True
            
        return False
    
    def isparent(self):
        "unittested"
        for m in self.member:
            if m is None: continue
            if re.match('.*parent.*', m, flags=re.I):
                return True
            
        return False
            
        
class Unit():
    def __init__(self):
        self.unitid = None        
        self.num = None
        self.denom = None
        
    def unitstr(self):
        "unittested"
        ret = self.num
        if self.denom is not None:
            ret += '/' + self.denom
        return ret
    
    def __str__(self):
        "unittested"
        return self.unitstr()
    
class FootNote():
    def __init__(self):
        self.footnote = None
        self.footnoteid = None
    

class XbrlParser(object):
    def __init__(self):
        self.nsmapi = None
        pass
    
    def parse_fact(self, factelem):
        """
        factelem - Element from lxml.etree
        return Fact class
        """
        
        f = Fact()
        f.tag = re.sub('{.*}', '', factelem.tag)
        f.version = self.nsmapi[re.sub('{|}', '', factelem.tag.replace(f.tag, ''))]
        
        for attr, value in factelem.attrib.items():
            attr = re.sub('{.*}','', attr)
            value = value.strip()
            if attr == 'contextRef':
                f.context = value
            if attr == 'id':
                f.factid = value
            if attr == 'decimals':
                if value in {'INF', 'inf'}:
                    f.decimals = '0'
                else:
                    f.decimals = value
            if attr == 'unitRef':
                f.unitid = value
        
        f.value = factelem.text        
        
        return f        
    
    def parse_context(self, contextelem):
        """
        contextelem - Element from lxml.etree
        return Context class
        """
        
        c = Context()
        for i in contextelem.iter('{*}identifier'):
            c.entity = int(i.text.strip())
        
        for d in contextelem.iter('{*}startDate'):            
            c.sdate = utils.str2date(d.text.strip())
        
        for d in contextelem.iter('{*}endDate'):            
            c.edate = utils.str2date(d.text.strip())
            
        for d in contextelem.iter('{*}instant'):            
            c.edate = utils.str2date(d.text.strip())
            
        c.contextid = contextelem.attrib['id'].strip()
        
        for ex in contextelem.iter('{*}explicitMember'):
            c.dim.append(ex.attrib['dimension'])
            c.member.append(ex.text.strip())
        
        return c
    
    def parse_footnote(self, footelem):
        """
        contextelem - Element from lxml.etree
        return FootNote class
        """
        
        fn = FootNote()
        if footelem.text is None:
            fn.footnote = ''
        else:
            fn.footnote = footelem.text.strip()
            
        fn.footnoteid = footelem.attrib['{%s}label'%footelem.nsmap['xlink']]
        
        return fn
        
    
    def parse_unit(self, unitelem):
        """
        unitelem - Element from lxml.etree
        return Unit class
        """
        u = Unit()
        
        u.unitid = unitelem.attrib['id'].strip()
        div = unitelem.find('{*}divide')
        if div is not None:
            num = div.find('{*}unitNumerator').find('{*}measure')
            denom = div.find('{*}unitDenominator').find('{*}measure')
            u.num = re.sub('.*:', '', num.text).lower()
            u.denom = re.sub('.*:', '', denom.text).lower()
        else:
            m = unitelem.find('{*}measure')
            u.num = re.sub('.*:', '', m.text).lower()
            
        return u
    
    def parse_units(self, root):
        """
        return dict {unitid:Unit()}
        """
        units = {}
        for unitelem in root.iter('{*}unit'):
            u = self.parse_unit(unitelem)
            units[u.unitid] = u
            
        return units
    
    def parse_facts(self, root, ignore_textblocks=True):
        """
        return list: [Fact()]
        """
        self.nsmapi = {v:k for k,v in root.nsmap.items()}
        facts = []
        
        for factelem in root.findall('.//*[@unitRef]'):
            if (ignore_textblocks and 
               factelem.tag.lower().endswith('textblock')):
                continue
            
            f = self.parse_fact(factelem)
            facts.append(f)            
            
        return facts
            
    def parse_contexts(self, root):
        """
        return dict: {contextid:Context()}
        """
        contexts = {}
        for contextelem in root.iter('{*}context'):
            c = self.parse_context(contextelem)
            contexts[c.contextid] = c
        
        return contexts
    
    def parse_footnotes(self, root):
        """
        return dect: {factid:FootNote()}
        """
        fnlink = root.find('{*}footnoteLink')
        if fnlink is None:
            return {}
        
        footnotes = {}
        for footelem in fnlink.findall('{*}footnote'):            
            fn = self.parse_footnote(footelem)
            footnotes[fn.footnoteid] = fn
            
        factids = {}
        for loc in fnlink.findall('{*}loc'):
            href = re.sub('.*#', '', loc.attrib['{%s}href' % loc.nsmap['xlink']])
            label = loc.attrib['{%s}label' % loc.nsmap['xlink']]
            factids[label] = href
        
        arcs = {}
        for arc in fnlink.findall('{*}footnoteArc'):
            fr = arc.attrib['{%s}from' % loc.nsmap['xlink']]
            to = arc.attrib['{%s}to' % loc.nsmap['xlink']]
            arcs[factids[fr]] = footnotes[to]
            
        return arcs
    
    def parse_dei(self, root, units):
        """return dict object with main Document Entity Facts
        {'fye': [[fey, context], ...],
           'period': [[period, context], ...],
           'shares': [[shares, context], ...],
           'fy': [[fy, context], ...],
           'cik': [[cik, context], ...],
           'us-gaap':'yyyy-mm-dd'}
        """
        "unittested"
        
        dei = {'fye': [],
               'period': [],
               'shares': [],
               'fy': [],
               'cik': []}
        if 'dei' not in root.nsmap:
            raise XbrlException('no dei section in xbrl file')
        if 'us-gaap' not in root.nsmap:
            raise XbrlException('taxonomy doesnt definded')
        
        dei['us-gaap'] = root.nsmap['us-gaap'].split('/')[-1]
        
        for e in root.iter('{%s}*' % root.nsmap['dei']):
            if e.text is None:
                continue
            
            tag = re.sub('{.*}', '', e.tag.strip())                        
            text = e.text.strip()
            unit = e.attrib.get('unitRef', None)
            context = e.attrib.get('contextRef', None)
            if unit is not None and units[unit].unitstr() == 'shares':
                dei['shares'].append([text, context])
                continue
            
            if tag == 'DocumentFiscalYearFocus':
                dei['fy'].append([text, context])
            if tag == 'CurrentFiscalYearEndDate':
                dei['fye'].append([text, context])
            if tag  == 'DocumentPeriodEndDate':
                dei['period'].append([text, context])
            if tag == 'EntityCentralIndexKey':
                dei['cik'].append([text, context])
                
        return dei
    
    def parse_textblocks(self, root, text_blocks):
        """Return list[{"name": text_block_name,
                        "context": str,
                        "value": str
                        }
                      ]
        """
        "unittested"
        data = []
        for name in text_blocks:
            for e in root.iter('{*}'+ name):
                data.append({'name': name,
                             'context': e.attrib['contextRef'],
                             'value': e.text.strip()
                                            .replace('\r', '')
                                            .replace('\n', '')                                             
                            })
            
        return(data)
    
class xbrltrans():
    @staticmethod        
    def transform_facts(facts):
        tfacts = {}
        for f in facts:
            key = f.key()
            if key in tfacts:
                tfacts[key].update(f)
            else:
                tfacts[key] = f
        return tfacts
    
    @staticmethod
    def transform_fn(facts, footnotes):
        fn = {}
        for key, f in facts.items():
            if f.factid in footnotes:
                fn[key] = footnotes[f.factid]
                
        return fn        
    
class XbrlCleanUp():
    def __init__(self):
        self.currencies = [None, 'usd', 'eur', 'cad', 'shares', 'pure']
        pass
    
    def cleanup(self, facts, units, contexts, footnotes):
        units = self.cleanupunits(units)
        facts = self.cleanupfacts(facts, units)
        #pure contexts needed when parse TextBlocks and other 
        #nonnumeric facts
        #contexts = self.cleanupcontexts(contexts, facts)
        footnotes = self.cleanupfn(footnotes, facts)
        
        return facts, units, contexts, footnotes
    
    def cleanupfacts(self, facts, units):
        facts_cu = {}
        for key, f in facts.items():
            if f.unitid not in units or f.value is None:
                continue            
                
            f.decimalize()
            facts_cu[key] = f
                
        return facts_cu
    
    def cleanupunits(self, units):
        units_cu = {}
        for u in units.values():
            if (u.num in self.currencies and u.denom in self.currencies):
                units_cu[u.unitid] = u
                
        return units_cu
    
    def cleanupcontexts(self, contexts, facts):
        fact_contexts = set([f.context for f in facts.values()])
        contexts_cu = {}
        for c in contexts.values():
            if c.contextid in fact_contexts:
                contexts_cu[c.contextid] = c
                
        return contexts_cu
    
    def cleanupfn(self, footnotes, facts):
        footnotes_cu = {}
        for key, f in facts.items():
             fn = footnotes.get(key, None)
             if fn:
                 footnotes_cu[f.key()] = fn
                 
        return footnotes_cu

if __name__ == '__main__':
    pass
    
    
    
    
    
        
    
    
    