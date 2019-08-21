# -*- coding: utf-8 -*-
"""
Created on Tue Mar 19 12:29:38 2019

@author: Asus
"""

import pandas as pd
import numpy as np
import json
from abc import ABCMeta, abstractmethod

import indi.indprocs as procs
from indi.modelspool import ModelsPool
from indi.exceptions import IndiException
from utils import class_for_name

class CalcProcedure(object):
    def __init__(self, script):
        self.script = "def calculate(params, fy):\n"

        for l in script.split("\n"):
            self.script += "    " + l + "\n"

        self.script += "    return result"

    def run_it(self, params, fy):
        exec(self.script)
        retval = locals()["calculate"](params, fy)
        return retval

class Indicator(metaclass=ABCMeta):
    header = ['adsh', 'fy','pname','sname','o', 'w', 'class', 'ord', 'value']
    def __init__(self, name, one_time):
        self.dp = set()
        self.name = name
        self.one_time = (one_time==1)        

    def dependencies(self):
        return set([name for name in self.dp if not name.startswith('us-gaap')])
    
    @abstractmethod
    def calc(self, nums: pd.DataFrame, fy: int, adsh: str) -> None:
        pass
    
    @abstractmethod
    def description(self) -> str:
        pass
    

class IndicatorProcedural(Indicator):
    def __init__(self, name: str, one_time: bool) -> None:
        super().__init__(name, one_time)
        
        self.proc : procs.CalcBaseClass
        self.proc = class_for_name('indi.indprocs', name)()
        
        self.dp = self.proc.dp()
        return

    def description(self):
        import inspect
        return inspect.getsource(self.proc.__class__)

class IndicatorStatic(IndicatorProcedural):
    def calc(self, nums, fy, adsh):
        nums = nums[(nums['fy'] == fy) & (nums['name'].isin(self.dp))]
        
        params = nums.pivot(index='fy', columns='name', values='value')

        for d in self.dp:
            if d not in params.columns:
                params[d] = np.nan

        result = np.nan
        if params.shape[0] != 0:
            result = self.proc.run_it(params.loc[fy], fy)

        n = (params.stack(dropna=False)
                   .reset_index()
                   .rename(columns={'name':'sname', 0:'value'}))
        n['w'] = 1.0
        n['ord'] = n.index
        n['o'] = 0
        n['pname'] = self.name
        n['fy'] = fy
        n['class'] = 1
        n['adsh'] = adsh
 
        return result, n[Indicator.header]

class IndicatorDynamic(IndicatorProcedural):
    def calc(self, nums, fy, adsh):
        n = nums.loc[nums['name'].isin(self.dp)]
        
        n = (n.pivot(index='fy', columns='name', values='value')
                .reset_index()
                .sort_values('fy', ascending=False)
                )
        for d in self.dp:
            if d not in n.columns:
                n[d] = np.nan

        result = np.nan
        if n.shape[0] != 0:
            result = self.proc.run_it(n, fy)

        n = (n.set_index('fy')
              .stack(dropna=False)
              .reset_index()
              .rename(columns={'name':'sname', 0:'value'}))
        
        n['w'] = 1.0
        n['ord'] = n.index
        n['o'] = 0
        n['pname'] = self.name
        n['class'] = 1.0
        n['adsh'] = adsh
            
        return result, n[Indicator.header]

class IndicatorRestated(Indicator):
    def __init__(self, name, classifier, class_id):
        super().__init__(name, False)
        self.classifier, self.class_id = classifier, class_id

    def calc(self, nums, fy, adsh):
        #merge structure and nums in one table p
        n = nums.loc[nums['fy'] == fy]
        
        p = self.classifier.predicted
        p = p.merge(n, 'left', left_on=['c', 'v'], right_on=['tag', 'version'])
        
        assert id(p) != id(self.classifier.predicted)
        
        #filter and calc result
        r = p[(p['class'] == self.class_id) & (p['l'] == True)]
        r = r.dropna()
        
        result = np.nan
        if r.shape[0] != 0:
            r['weighted'] = r['value']*r['w']
            result = r['weighted'].sum()

        p = p.reset_index()
        
        p.fillna(inplace=True, value={'adsh': adsh, 'fy': fy})        
        p['pname'] = self.name        
        p['sname'] = p['v'] + ':' + p['c']
        p = p.rename({'index': 'ord'}, axis='columns')
        
        return result, p[Indicator.header + ['l']]

    def description(self):
        
        return 'indicator:{0}\nclass id: {1}\n{2}'.format(
                self.name,
                self.class_id,
                self.classifier.description())


def create(ind_name: str,
           class_pool: ModelsPool,
           kwargs) -> Indicator:
    if kwargs['type'] == 'restated':
        classifier = class_pool.get_classifier(kwargs['fmodel'])
        return IndicatorRestated(name=ind_name, 
                                 classifier=classifier,
                                 class_id=kwargs['class_id'])
    else:
        if kwargs['type'] == 'static':
            return IndicatorStatic(name=ind_name,
                                   one_time=kwargs['one_time'])
        elif kwargs['type'] == 'dynamic':
            return IndicatorDynamic(name=ind_name,
                                   one_time=kwargs['one_time'])
        else:
            raise IndiException('unsupported type: {0}'.format(kwargs['type']))        
    
def indicator_scripts():
    import mysqlio.basicio as do
    import re
    
    with open('mgparams_scripts.py', 'w') as file, \
            do.OpenConnection() as con:
        
        tab = "    "
        tab2 = tab + tab
        tab3 = tab2 + tab
        linesep = '\n'
        r = re.compile('\s+')
        
        file.write("import numpy as np" + linesep)
        file.write('import pandas as pd' + linesep)
                
        cur = con.cursor(dictionary=True)
        cur.execute('select * from mgparams')

        
        for row in cur.fetchall():  
            file.write(linesep)
            text = "class {0}(CalcBaseClass):{1}".format(row['tag'], linesep)
            
            text += tab + "def __init__(self):" + linesep
            dep = set(json.loads(row['dependencies']).keys())
            text += tab2 + "self._dp = " + str(dep) + linesep
            text += linesep
            
            text += tab + "def run_it(self, params, fy):" + linesep            
            script = row['script']
            for line in script.split('\n'):
                line = line.replace('\r', '')
                m = r.match(line)
                line = line.strip()
                start = ''
                if m is not None:
                    t = int(len(m[0])/4)
                    if t == 1:
                        start = tab
                    if t == 2:
                        start = tab2
                    if t == 3:
                        start = tab3
                        
                text += tab2 + start + line + linesep
            
            text += linesep
            text += tab2 + "if not np.isnan(result):" + linesep
            text += tab3 + "return float(result)" + linesep
            text += tab2 + "else:" + linesep
            text += tab3 + "return result" + linesep
            
            file.write(text)
        
if __name__ == '__main__':
    pass



