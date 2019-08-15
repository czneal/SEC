# -*- coding: utf-8 -*-
"""
Created on Wed Aug  7 17:48:55 2019

@author: Asus
"""
from typing import Dict, List

from indi.lstmclass import ModelClassifier, create
from indi.loader import CSVLoader
from indi.exceptions import SortException

class ClassifierOrder():
    def __init__(self):
        self.__depth = 50
        self.__steps = 50
        self.__order = [] #type: List[str]
            
    def _one_step(self, c: ModelClassifier) -> None:
        self.__steps -= 1
        if self.__steps == 0:
            raise SortException('impossible order classifiers')
        
        if c.fmodel in self.__order:
            return
        
        if c.filter_model is None:
            self.__order.append(c.fmodel)
            return
            
        self._one_step(c.filter_model)
        self.__order.append(c.fmodel)
        
    def order(self, classifiers : Dict[str, ModelClassifier]) -> List[str]:
        self.__depth = len(classifiers)
        for c in classifiers.values():
            self.__steps = self.__depth
            self._one_step(c)
        
        return self.__order.copy()
            
class ModelsPool(object):
    def __init__(self):
        self.__order = [] #type: List[str]
        self.__pool = {} #type: Dict[str, ModelClassifier]
    
    def _load_pool(self, loader: CSVLoader) -> None:
        #create models
        for model_name, model_kwargs in loader.models():
            c = create(model_name, model_kwargs)
            self.__pool[model_name] = c
            
        #setup filter_model and filter_id
        for model_name, model in self.__pool.items():
            filter_ = loader.get_filter(model_name)
            if filter_ is None:
                continue
            model.filter_model = self.__pool[filter_['filter_model']]
            model.filter_id = int(filter_['answer_id'])
                
    def load(self, loader: CSVLoader) -> None:
        self._load_pool(loader)
        o = ClassifierOrder()
        self.__order = o.order(self.__pool)
                
    def predict_all(self, structure) -> None:
        for name in self.__order:
            self.__pool[name].predict_all(structure)            
    
    def get_classifier(self, model_name) -> ModelClassifier:
        c = self.__pool[model_name]
        return c

#m = SingleParentAndChild(fmodel = 'mgparams/liabilities_class_pch_v2019-03-13.h5',
#                    fdict = 'mgparams/dictionary.csv',
#                    max_len = 40,
#                    start_chapter = 'bs',
#                    start_tag = 'us-gaap:LiabilitiesAndStockholdersEquity',
#                    filter_model = None)
#m.predict_all(structure)
#df = m.predicted
            
if __name__ == "__main__":    
    pass

