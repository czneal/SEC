from typing import List
import json

from xbrlxml.xbrlexceptions import XbrlException
from classifiers.mainsheets import MainSheets
import algos.scheme
import algos.xbrljson
import xbrlxml.truevalues as tv

class ChapterChooser(object):
    """Choose only main seets from xbrl schemas    
    """
    def __init__(self, xbrlfile):
        self.xbrlfile = xbrlfile
        self.mschapters = None
        
    def choose(self) -> None:            
        xsd = self.xbrlfile.schemes['xsd']
        pres = self.xbrlfile.schemes['pres']
        
        mschapters = {str(index) + ' ' + xsd[roleuri].label: roleuri 
                     for index, roleuri in enumerate(pres)
                         if roleuri in xsd and xsd[roleuri].sect == 'sta'
                             and pres[roleuri].gettags()}
        ms = MainSheets()
        priority, labels = [], []
        for label, roleuri in mschapters.items():
            labels.append(label)
            priority.append(len(pres[roleuri].gettags()))
        
        true_labels = (tv.TRUE_VALUES
                         .get_true_chapters(self.xbrlfile.adsh))
        if true_labels is None:
            labels = ms.select_ms(labels, 
                                  priority=priority,
                                  indicator=self.xbrlfile.company_name)
        else:
            labels = true_labels
        
        if len(labels) > 3:
            msg = json.dumps(list(zip(mschapters.keys(), priority)))
            raise XbrlException('count of main sheets > 3' + '\n' + msg)
            
        self.mschapters = {labels[label]:mschapters[label] 
                            for label in labels}
        pass

class ContextChooser(object):
    """Choose context for specific roleuri    
    """
    def __init__(self, xbrlfile):
        self.xbrlfile = xbrlfile
        
        
    def choose(self, roleuri: str) -> str:
        pres = self.xbrlfile.schemes['pres'].get(roleuri, None)
        if pres is None:
            raise XbrlException('unable find context if chapter has not presentation view')
        
        defi = self.xbrlfile.schemes['defi'].get(roleuri, pres)        
        
        chdim = set(pres.dims())
        chcontexts = set([c.contextid 
                          for c in self.xbrlfile.contexts.values() 
                            if len(set(c.dim).difference(chdim)) == 0])
        
        dfacts = self.xbrlfile.dfacts
        f = dfacts[dfacts['context'].isin(chcontexts) &
                   dfacts['name'].isin(pres.gettags())]
        
        f = (f.groupby('context')['name']
                      .count()
                      .sort_values(ascending=False)
                      .reset_index()
                      .rename(index=str, columns={'name':'cnt'}))
        
        return self._choosecontext(f, thres = 0.5)
    
    def _choosecontext(self, f, thres = 0.5) -> str:
        if f.shape[0] == 0:
            return None
        
        contexts = self.xbrlfile.contexts
        nondim, successor, parent = None, None, None                
        top = (f.iloc[0]['context'], f.iloc[0]['cnt'])
        
        for index, row in f.iterrows():
            if nondim is None and not contexts[row['context']].isdimentional():
                nondim = (row['context'], row['cnt'])
                if nondim[1]/top[1] < thres:
                    nondim = None
            if successor is None and contexts[row['context']].issuccessor():
                successor = (row['context'], row['cnt'])
                if successor[1]/top[1] < thres:
                    successor = None
            if parent is None and contexts[row['context']].isparent():
                parent = (row['context'], row['cnt'])
                if parent[1]/top[1] < thres:
                    parent = None
        
        if successor:
            return successor[0]
        if nondim:
            return nondim[0]
        if parent:
            return parent[0]
        
        return top[0]

class ChapterExtender(object):
    """Extend chapter calculation scheme
    """
    def __init__(self, xbrlfile):
        self.xbrlfile = xbrlfile
        self.extentions = []
        self.roleuri = None
    
    def check(self, ext_roleuri: str, node_label: str, context: str) -> bool:        
        pres = self.xbrlfile.schemes['pres'].get(ext_roleuri, None)
        calc = self.xbrlfile.schemes['calc'].get(ext_roleuri, pres)
        dfacts = self.xbrlfile.dfacts
        
        tags_iter = algos.scheme.enum(
                        structure = calc.nodes[node_label], 
                        outpattern='c')
        tags = set([t for [t] in tags_iter])
        
        f = dfacts[dfacts['name'].isin(tags) & 
                   (dfacts['context'] == context)]
        if f.shape[0] < len(tags):
            return False
        else:
            return True
    
    def find_extentions(self, roleuri: str) -> List[str]:        
        self.roleuri = roleuri
        self.extentions = []
        
        if self.roleuri not in self.xbrlfile.schemes['calc']:
            self.extentions = []
            return []
        
        xsd = self.xbrlfile.schemes['xsd']
        pres = self.xbrlfile.schemes['pres']
        calc = self.xbrlfile.schemes['calc']
        xsds = [roleuri for roleuri, c in xsd.items() if c.sect != 'sta']
        pres = [roleuri for roleuri in xsds if roleuri in pres]
        
        extentions, warnings = algos.scheme.find_extentions(
                                self.roleuri,
                                calc, pres, xsds)
        context_chooser = ContextChooser(self.xbrlfile)
        for node_label, ext_roleuri in extentions.items():
            context = context_chooser.choose(ext_roleuri)
            if self.check(ext_roleuri, node_label, context):
                self.extentions.append({'roleuri': ext_roleuri,
                                        'context': context,
                                        'label': node_label})
            else:
                msg = {'message': 'extention fails, not all children present in fact section of XBRL file',
                       'base chapter': self.roleuri,
                       'ext chapter' : ext_roleuri,
                       'node in ext chapter': node_label                       
                      }
                warnings.append(json.dumps(msg, indent=3))
        return warnings
    
    def extend(self):
        extentions = {d['label']: d['roleuri'] for d in self.extentions}
        algos.scheme.extend_clac_scheme(self.roleuri, 
                                        self.xbrlfile.schemes['calc'],
                                        extentions)