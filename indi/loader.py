# -*- coding: utf-8 -*-

from typing import Dict, Tuple, Union, Optional, Generator, TypeVar
import pandas as pd
from abc import ABCMeta, abstractmethod

from settings import Settings
from mysqlio.basicio import OpenConnection

StrInt = TypeVar('StrInt', str, int)

class Loader(metaclass=ABCMeta):
    def __init__(self):
        self._indc = None #type: pd.DataFrame
    
    @abstractmethod
    def load(self):
        pass
    
    def indicators(self) -> Generator[Tuple[str, Dict[str, StrInt]],
                                      None, None]:
        for ind_name, row in self._indc.iterrows():
            yield (ind_name, row.to_dict())
            
class CSVLoader(Loader):
    def __init(self):
        #load csv files with indicator and classifier settings
        super().__init__()
        
        self.__class = None #type: pd.DataFrame
        self.__filters = None #type: pd.DataFrame
        
    def load(self):
        model_dir = Settings.models_dir()
        self._indc = (pd.read_csv(model_dir + 'indicators.csv', sep='\t')
                            .drop_duplicates('indicator_name')
                            .set_index('indicator_name')
                            )
        self._indc = self._indc.where((pd.notnull(self._indc)), None)
        
        self.__class = (pd.read_csv(model_dir + 'classifiers.csv', sep='\t')
                            .drop_duplicates('fmodel')
                            .set_index('fmodel')
                            )
        self.__class = self.__class.where((pd.notnull(self.__class)), None)
        
        self.__filters = (pd.read_csv(model_dir + 'filters.csv', sep='\t')
                            .drop_duplicates('fmodel')
                            .set_index('fmodel')
                            )
        self.__filters = self.__filters.where((pd.notnull(self.__filters)), None)
        
            
    def get_class(self, model_name: str) -> Optional[Dict[str, StrInt]]:
        if model_name in self.__class.index:
            return self.__class.loc[model_name].to_dict()
        return None
    
    def get_filter(self, model_name: str) -> Optional[Dict[str, StrInt]]:
        if model_name in self.__filters.index:
            return self.__filters.loc[model_name].to_dict()
        return None
    
    def models(self) -> Generator[Tuple[str, Dict[str, StrInt]],
                                  None, None]:
        for model_name, row in self.__class.iterrows():
            yield (model_name, row.to_dict())
            
class DBLoader(Loader):
    def __init__(self):
        super().__init__()
        
    def load(self):
        with OpenConnection() as con:
            cur = con.cursor(dictionary=True)
            cur.execute('select * from mgparams')
            self._indc = pd.DataFrame(cur.fetchall()).set_index('tag')
            
    
if __name__ == "__main__":
    loader = DBLoader()
    loader.load()
    
    print([x for x in loader.indicators()])
    