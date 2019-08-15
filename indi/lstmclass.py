# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
import re
from abc import ABCMeta, abstractmethod
from typing import Optional, Dict, Union

from algos.scheme import enum
from settings import Settings
from indi.exceptions import ModelException
import indi.lstmmodels
from indi.types import StrInt

class Filter():
    def __init__(self, classifier, answer_id: int):
        self.classifier = classifier #type: ModelClassifier
        self.answer_id = answer_id #type: int
        
    def do_filter(self) -> pd.DataFrame:
        df = self.classifier.predicted
        df = df[df['class'] == self.answer_id]
        return df[list('pclwov')]
    
class ModelClassifier(metaclass=ABCMeta):
    def __init__(self, 
                 fdict: str, 
                 fmodel: str, 
                 max_len: int, 
                 start_chapter: str, 
                 start_tag: str,
                 leaf: bool):
        
        self.predicted = None #type: pd.DataFrame
        self.max_len = max_len 
        self.start_chapter = start_chapter
        self.start_tag = start_tag
        self._load_dict(fdict)
        self._load_model(fmodel)
        
        self.leaf = leaf
        self.filter_model = None #type: Optional[ModelClassifier]
        self.filter_id = 0 #type: int
        
        self.fmodel = fmodel.split('/')[-1]
        
    def description(self) -> str:
        return 'model: {0}\nchapter: {1}\ntag: {2}\nmax_len: {3}'.format(
                self.fmodel, self.start_chapter, self.start_tag,
                self.max_len)

    def _load_dict(self, filename: str) -> None:
        with open(Settings.models_dir() + filename) as f:
            self.tag_to_code = {l.split("\t")[0]:l.replace("\n","").split("\t")[1] for l in f}

    def _load_model(self, filename: str) -> None:
        m = indi.lstmmodels.Models().models            
        self.model = m[filename]
#        self.model = indi.lstmmodels.MODELS[filename]

    def _to_vector(self, tag: str, x: np.ndarray, row: int, pos: int) -> None:
        words = re.findall('[A-Z][^A-Z]*', tag)
        for i, word in enumerate(words):
            x[row, pos + i] = self.tag_to_code.get(word, 1)
    
    @abstractmethod    
    def _vectorize(self, parent: str, child: str, 
                        x: np.ndarray, row: int) -> None:
        pass
    
    @abstractmethod
    def _explain_results(self, x: np.ndarray) -> np.ndarray:
        pass

    def _prepare_predict(self, structure) -> None:
        if self.filter_model is not None:
            self.predicted = self.filter_model.predicted.copy()
            return
        
        self.predicted = pd.DataFrame([], columns=list('pclwov'))
        
        if self.start_chapter not in structure:
            return
        start = structure[self.start_chapter]['chapter']
        if self.start_tag is not None:
            if self.start_tag not in start.nodes:
                return
            else:
                start = start.nodes[self.start_tag]
        
        pairs = []
        for [p, c, l, w, o, v] in enum(start, leaf=False,
                              outpattern='pclwov', 
                              func=lambda x: x.tag):
            pairs.append((p, c, l, w, o, v))
            
        self.predicted = pd.DataFrame(pairs, columns=list('pclwov'))
        
    def _do_filter(self) -> None:
        self.w = pd.Series(data=True, index=self.predicted.index)
        
        if self.filter_model is not None:
            self.w = (self.predicted['class'] == self.filter_id)
            
        if self.leaf:
            self.w = ((self.w) & (self.predicted['l'] == True))
        
    def _predict(self) -> None:
        p = self.predicted.loc[self.w]
        
        if p.shape[0] == 0:
            self.predicted['class'] = np.nan
            return        
        
        x = np.ones((p.shape[0], self.max_len))
        for i, (_, row)  in enumerate(p.iterrows()):
            self._vectorize(row['p'], row['c'], x, i)
            
        y = self.model.predict(x)
        y = self._explain_results(y)

        self.predicted['class'] = np.nan
        self.predicted.loc[self.w, 'class'] = y
        
    def predict_all(self, structure) -> None:
        '''
        make prediction for all tags in structure
        '''
        self._prepare_predict(structure)
        self._do_filter()
        self._predict()
    
class SingleAnswer(ModelClassifier):
    def _explain_results(self, x):
        x = (x > 0.5).astype(int)
        return x
    
class MultiAnswer(ModelClassifier):
    def _explain_results(self, x):
        x = np.argmax(x, axis=1)
        return x.astype(int)
    
class ParentAndChild(ModelClassifier):
    def _vectorize(self, parent: str, child: str, 
                        x: np.ndarray, row: int) -> None:        
        
        self._to_vector(parent, x=x, row=row, pos=0)
        self._to_vector(child, x=x, row=row, pos=int(self.max_len/2))

class OnlyChild(ModelClassifier):
    def _vectorize(self, parent: str, child: str, 
                        x: np.ndarray, row: int) -> None: 
        self._to_vector(child, x=x, row=row, pos=0)

class SingleParentAndChild(SingleAnswer, ParentAndChild):
    pass

class MultiParentAndChild(MultiAnswer, ParentAndChild):
    pass

class MultiOnlyChild(MultiAnswer, OnlyChild):
    pass

class SingleOnlyChild(SingleAnswer, OnlyChild):
    pass

def create(model_name: str, 
           kwargs : Dict[str, StrInt]
           ) -> ModelClassifier:
    args = {'fdict': kwargs['fdict'],
            'fmodel': model_name,
            'max_len': kwargs['max_len'],
            'start_chapter': kwargs['start_chapter'],
            'start_tag': kwargs['start_tag'],
            'leaf': (kwargs['leaf'] == 1)}
    
    if kwargs['pc'] == 'pc':
        if  kwargs['multi'] == 0:
            return SingleParentAndChild(**args)
        elif kwargs['multi'] == 1:
            return MultiParentAndChild(**args)
        else:
            raise ModelException('unsupported type {0}'.format(kwargs['multi']))
    if kwargs['pc'] == 'c':
        if  kwargs['multi'] == 0:
            return SingleOnlyChild(**args)
        elif kwargs['multi'] == 1:
            return MultiOnlyChild(**args)
        else:
            raise ModelException('unsupported type {0}'.format(kwargs['multi']))
            
    raise ModelException('unsupported arguments')
    

if __name__ == '__main__':
    pass
    