# -*- coding: utf-8 -*-
"""
Created on Tue Mar 19 12:29:38 2019

@author: Asus
"""

import database_operations as do
import json
import pandas as pd
import numpy as np

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

class IndicatorPool(object):
    def __init__(self, class_pool):
        self.class_pool = class_pool
        self._init_indicators()
        return

    def _init_indicators(self):
        self.indicators = {}
        con = None
        #берем из базы данных все параметры процедурного типа
        try:
            con = do.OpenConnection()
            cur = con.cursor(dictionary=True)
            cur.execute('select * from mgparams')

            for row in cur:
                class_name = None
                if row['type'] == 'static':
                    class_name = IndicatorStatic
                else:
                    class_name = IndicatorDynamic

                self.indicators[row['tag']] = class_name(row['script'],
                                                   row['dependencies'],
                                                   row['tag'],
                                                   row['one_time'])
        except:
            raise
        finally:
            if con: con.close()

        #инициализируем параметры restated через class_pool
        df = pd.read_csv('mgparams/indicators.csv', sep='\t').set_index('indicator_name')
        for index, row in df.iterrows():
            self.indicators[index] = IndicatorRestated(index,
                           self.class_pool.pool[row['class_model']],
                           row['class_id'])

        #упорядочиваем их по иерархии
        self.sort_indicators()

        return

    def sort_indicators(self):
        self.indicator_order = []
        total = 50
        while (len(set(self.indicator_order).intersection(set(self.indicators.keys()))) !=
               len(set(self.indicators.keys()))):
            total -= 1
            if total == 0:
                sdiff = set(self.indicator_order).symmetric_difference(set(self.indicators.keys()))
                raise Exception("impossible to order indicators:{0}".format(sdiff))

            for ind_name, ind in self.indicators.items():
                dependencies = set()
                for dep_tag in ind.dp:
                    if dep_tag.startswith('us-gaap'): continue
                    if dep_tag in self.indicator_order: continue
                    dependencies.add(dep_tag)
                if len(dependencies) == 0 and ind_name not in self.indicator_order:
                    self.indicator_order.append(ind_name)

        return

    def calc(self, nums, fy_structure):
        """
        calculate indicators for one cik and all possible years
        fy_structure - dict {fy:[structure, adsh]}
        nums - DataFrame columns = [adsh, fy, value, tag]
        """

        data = []
        depends = []
        #first calculate restated indicators they are depends only on us-gaap tags
        for fy, (structure, adsh) in fy_structure.items():
            self.class_pool.predict_all(structure)
            for ind in self.indicator_order:
                if not self.indicators[ind].isrestated(): continue
                #print(ind, end='')
                value, n = self.indicators[ind].calc(nums, fy, adsh)
                #n['adsh'] = adsh
                data.append({'adsh':adsh,
                              'fy':fy,
                              'value':value,
                              'tag':ind})
                depends.append(n)
                #print(fy, value)
        nums = nums.append(data, ignore_index=True)


        #then all others
        year = max(fy_structure)
        for ind in self.indicator_order:
            if self.indicators[ind].isrestated(): continue
            for fy, (structure, adsh) in fy_structure.items():
                if self.indicators[ind].onetime and fy != year:
                    continue
                #print(ind, end = '')
                value, n = self.indicators[ind].calc(nums, fy, adsh)
                #n['adsh'] = adsh
                nums = nums.append({'adsh':adsh, 'value':value, 'fy':fy, 'tag':ind},
                                   ignore_index = True)
                depends.append(n)
                #print(fy, value)

        depends = pd.concat(depends)
        columns = depends.columns.tolist() + ['value'] + ['sadsh']
        depends = pd.merge(depends, nums[['fy', 'tag', 'value', 'adsh']],
                           left_on=['sname','fy'], right_on=['tag', 'fy'],
                           how='left',
                           suffixes=('', '_y')).rename({'adsh_y':'sadsh'}, axis='columns')
        depends['sadsh'] = depends['sadsh'].fillna(depends['adsh'])
        return nums, depends[columns]


class Indicator(object):
    def __init__(self, name, onetime):
        self.dp = set()
        self.name = name
        self.onetime = (onetime==1)
        self.dep_header = ['adsh', 'fy','pname','sname','offs', 'weight', 'class_id', 'ord']

    def dependecies(self):
        return self.dp.copy()

class IndicatorProcedural(Indicator):
    def __init__(self, script, depend, name, onetime):
        super().__init__(name, onetime)
        self.dp = set(e for e in json.loads(depend))
        self.proc = CalcProcedure(script)
        return

    def isrestated(self):
        return False

    def description(self):
        return self.name, str(self.dp) + '\n\n' + self.proc.script

class IndicatorStatic(IndicatorProcedural):
    def calc(self, nums, fy, adsh):
        nums = nums[(nums['fy'] == fy) & (nums['tag'].isin(self.dp))]
        params = nums.pivot(index='fy', columns='tag', values='value')

        for d in self.dp:
            if d not in params.columns:
                params[d] = np.nan

        result = np.nan
        if params.shape[0] != 0:
            result = self.proc.run_it(params.loc[fy], fy)

        n = params.stack(dropna=False).reset_index().rename(columns={'tag':'sname', 0:'value'})
        n['weight'] = 1.0
        n['ord'] = n.index
        n['offs'] = ''
        n['pname'] = self.name
        n['fy'] = fy
        n['class_id'] = 1
        n['adsh'] = adsh

        return result, n[self.dep_header]

    def isstatic(self):
        return True

class IndicatorDynamic(IndicatorProcedural):
    def __init__(self, script, depend, name, onetime):
        super().__init__(script, depend, name, onetime)
        params = json.loads(depend)
        self.duration = -1
        for k in params:
            if params[k] is not None:
                self.duration = params[k]
        return

    def calc(self, nums, fy, adsh):
        n = nums[nums['tag'].isin(self.dp)]
        n = (n.pivot(index='fy', columns='tag', values='value')
                .reset_index()
                .sort_values('fy', ascending=False)
                )
        for d in self.dp:
            if d not in n.columns:
                n[d] = np.nan

        result = np.nan
        if n.shape[0] != 0:
            result = self.proc.run_it(n, fy)

        n = n.set_index('fy').stack(dropna=False).reset_index().rename(columns={'tag':'sname', 0:'value'})
        n['weight'] = 1.0
        n['ord'] = n.index
        n['offs'] = ''
        n['pname'] = self.name
        n['class_id'] = 1.0
        n['adsh'] = adsh

        return result, n[self.dep_header]

    def isstatic(self):
        return False

class IndicatorRestated(Indicator):
    def __init__(self, name, classifier, class_id):
        super().__init__(name, False)
        self.classifier, self.class_id = classifier, class_id

        return

    def calc(self, nums, fy, adsh):
        n = nums[nums['fy'] == fy]
        df = self.classifier.predicted
        f = df[df['class_id'] == self.class_id]
        r = f.merge(n, left_on='child', right_on='tag')
        r = r.dropna()

        result = np.nan
        if r.shape[0] != 0:
            r['weighted'] = r['value']*r['w']
            result = r['weighted'].sum()

        stab = self.classifier.structure_tab
        stab = stab.merge(f, left_on = 'sname', right_on='child', how='left')
        stab['fy'] = fy
        stab['pname'] = self.name
        stab['adsh'] = adsh

        stab = stab[self.dep_header].rename({'w':'weight'}, axis='columns')

        return result, stab

    def isstatic(self):
        return True

    def isrestated(self):
        return True

    def description(self):
        return (self.name,
                'class id: {0}\n{1}'.format(self.class_id,
                                       self.classifier.description()))


def indicator_scripts():
    con = None
    file = None
    #берем из базы данных все параметры процедурного типа

    try:
        file = open('mgparams/mgparams_scripts.py','w')
        file.write("import numpy as np\n")
        file.write('import pandas as pd\n')
        file.write('null = None\n')
        file.write('fy = 2016\n\n')

        con = do.OpenConnection()
        cur = con.cursor(dictionary=True)
        cur.execute('select * from mgparams')

        for row in cur:
            file.write('#{0}, {1}, {2}\n'.format(row['tag'], row['type'], row['one_time']))
            file.write('params = {0}\n\n'.format(row['dependencies']))
            file.write('{0}\n'.format(row['script']))
            file.write('\n\n')
    except:
        raise
    finally:
        if con: con.close()
        if file: file.close()



