# -*- coding: utf-8 -*-

import datetime as dt
from typing import Dict, Union, Mapping, Optional
import json
import os

import utils

TRUE_VALUES = None

class TrueValues(object):
    def __init__(self, source_filenames: Mapping[str, str]):
        self.periods = {} # type: Dict[str, dt.date]
        self.chapters = {} # type: Dict[str, Dict[str, str]]
        
        with open(source_filenames['periods']) as f:
            for line in f.readlines():
                columns = line.replace('\n','').replace('\r','').split('\t')
                self.periods[columns[0]] = utils.str2date(columns[1])
        
        with open(source_filenames['chapters']) as f:
            for line in f.readlines():
                columns = line.replace('\n','').replace('\r','').split('\t')
                self.chapters[columns[0]] = json.loads(columns[1])
    
    def get_true_period(self, adsh: str) -> Union[dt.date, None]:
        return self.periods.get(adsh, None)
    
    def get_true_chapters(self, adsh: str) -> Optional[Mapping[str, str]]:
        return self.chapters.get(adsh, None)

if TRUE_VALUES is None:    
    TRUE_VALUES = TrueValues({'periods': (os.path.split(__file__)[0] + 
                                          '/truevalues_periods.csv'),
                              'chapters': (os.path.split(__file__)[0] + 
                                           '/truevalues_chapters.csv')
                              })
    
if __name__ == "__main__":
    print(TRUE_VALUES.get_true_chapters('0001534504-19-000012'))
