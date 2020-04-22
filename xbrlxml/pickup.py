import json
from typing import Dict, List, Optional, Set, Tuple

import algos.scheme
import algos.xbrljson
import logs
import xbrlxml.truevalues as tv
from classifiers.mainsheets import MainSheets
from xbrlxml.xbrlexceptions import XbrlException
from xbrlxml.xbrlfile import XbrlFile


class ChapterChooser(object):
    """Choose only main seets from xbrl schemas
    """

    def __init__(self, xbrlfile: XbrlFile):
        self.xbrlfile = xbrlfile
        self.mschapters: Dict[str, str] = {}

    def choose(self) -> Dict[str, str]:
        """
        return dictionary
        {'bs': roleuri, - for Balance Sheet
         'is': roleuri, - for Income Statement,
         'cf': roleuri, - for Cash Flow,
         'se': roleuri, - for Stockholders Equity}
        raise XbrlException if one of main sheet present more than once
        """

        xsd = self.xbrlfile.xsd
        pres = self.xbrlfile.pres

        mschapters: Dict[str, str] = {str(index) + ' ' + xsd[roleuri].label: roleuri
                                      for index, roleuri in enumerate(pres)
                                      if roleuri in xsd and xsd[roleuri].sect == 'sta'
                                      and pres[roleuri].gettags()}
        ms = MainSheets()
        priority: List[int] = []
        label_list: List[str] = []
        for label, roleuri in mschapters.items():
            label_list.append(label)
            priority.append(len(pres[roleuri].gettags()))

        true_labels = (tv.TRUE_VALUES
                         .get_true_chapters(self.xbrlfile.adsh))
        if true_labels is None:
            labels = ms.select_ms(label_list, priority=priority)
        else:
            labels = true_labels

        check_up: Dict[str, int] = {}
        for v in labels.values():
            check_up[v] = check_up.setdefault(v, 0) + 1

        if not all([v <= 1 for v in check_up.values()]):
            extra = {'details': 'count of main sheets > {0}'.format(
                len(ms.sheets())),
                'labels': list(zip(mschapters.keys(), priority))}
            logs.get_logger(__name__).error(msg='main sheet choose failed',
                                            extra=extra)
            raise XbrlException('main sheet choose failed')

        self.mschapters = {labels[label]: mschapters[label]
                           for label in labels}
        return self.mschapters


class ContextChooser(object):
    """Choose context for specific roleuri
    """

    def __init__(self, xbrlfile):
        self.xbrlfile = xbrlfile

    def choose(self, roleuri: str) -> Optional[str]:
        pres = self.xbrlfile.pres.get(roleuri, None)
        if pres is None:
            msg = 'unable find context if chapter has not presentation view'
            logs.get_logger(__name__).error(msg=msg)
            raise XbrlException(msg)

        defi = self.xbrlfile.defi.get(roleuri, pres)

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
             .rename(index=str, columns={'name': 'cnt'}))

        return self._choosecontext(f, thres=0.5)

    def _choosecontext(self, f, thres=0.5) -> Optional[str]:
        if f.shape[0] == 0:
            return None

        contexts = self.xbrlfile.contexts
        nondim: Optional[Tuple[str, float]] = None
        successor: Optional[Tuple[str, float]] = None
        parent: Optional[Tuple[str, float]] = None
        top = (f.iloc[0]['context'], f.iloc[0]['cnt'])

        for index, row in f.iterrows():
            if nondim is None and not contexts[row['context']].isdimentional():
                nondim = (row['context'], row['cnt'])
                if nondim[1] / top[1] < thres:
                    nondim = None
            if successor is None and contexts[row['context']].issuccessor():
                successor = (row['context'], row['cnt'])
                if successor[1] / top[1] < thres:
                    successor = None
            if parent is None and contexts[row['context']].isparent():
                parent = (row['context'], row['cnt'])
                if parent[1] / top[1] < thres:
                    parent = None

        if successor:
            return str(successor[0])
        if nondim:
            return str(nondim[0])
        if parent:
            return str(parent[0])

        return str(top[0])


class ChapterExtender(object):
    """Extend chapter calculation scheme
    """

    def __init__(self, xbrlfile):
        self.xbrlfile = xbrlfile
        self.extentions = []
        self.roleuri = None

    def check(self,
              ext_roleuri: str,
              node_label: str,
              context: Optional[str]) -> bool:
        pres = self.xbrlfile.pres.get(ext_roleuri, None)
        calc = self.xbrlfile.calc.get(ext_roleuri, pres)
        dfacts = self.xbrlfile.dfacts

        tags_iter = algos.scheme.enum(structure=calc.nodes[node_label],
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

        if self.roleuri not in self.xbrlfile.calc:
            self.extentions = []
            return []

        xsd = self.xbrlfile.xsd
        pres = self.xbrlfile.pres
        calc = self.xbrlfile.calc
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
                msg = {
                    'message': 'extention fails, not all children present in fact section of XBRL file',
                    'base chapter': self.roleuri,
                    'ext chapter': ext_roleuri,
                    'node in ext chapter': node_label}
                warnings.append(json.dumps(msg, indent=3))
        return warnings

    def extend(self):
        extentions = {d['label']: d['roleuri'] for d in self.extentions}
        algos.scheme.extend_clac_scheme(self.roleuri,
                                        self.xbrlfile.calc,
                                        extentions)
