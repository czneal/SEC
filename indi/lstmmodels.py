# -*- coding: utf-8 -*-

from keras.models import load_model
from typing import Dict, Any

from settings import Settings

class Models():
    __models = None
    def __init__(self):
        if Models.__models is None:
            Models.__models = load_models()
    
    @property        
    def models(self):
        return self.__models
    
def load_models() -> Dict[str, Any]:
    print('load LSTM models...')
    files = ['liabilities_class_pch_v2019-03-13.h5',
             'liabilities_curr_noncurr_v2019-03-13.h5',
             'assets_multiclass_pch_v2019-03-13.h5',
             'assets_curr_noncurr_pch_v2019-03-13.h5',
             'income_st_multiclass_pch_v2019-03-13.h5',
             'cashflow_st_cashtype_pch_v2019-03-13.h5',
             'cashflow_st_multiclass_pch_v2019-03-14.h5',
             'all_gaap_tags_binary_v2019-03-13.h5']
    models = {}
    for file in files[:]:
        models[file] = load_model(Settings.models_dir() + file)
        
    print('LSTM models loaded')
    return models