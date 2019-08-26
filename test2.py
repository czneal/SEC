import json

import mysqlio.basicio as do
from algos.xbrljson import custom_decoder
from indi.indicators import IndicatorRestated
from indi.lstmclass import SingleOnlyChild, MultiParentAndChild

if __name__ == '__main__':
    adsh = '0000016875-19-000017'
    structures = do.read_report_structures([adsh])
    structure = json.loads(structures.loc[adsh]['structure'],
                           object_hook=custom_decoder)
    nums = do.read_reports_nums([adsh])
    
    fclassi = SingleOnlyChild(fdict='dictionary.csv', 
                             fmodel='all_gaap_tags_binary_v2019-03-13.h5', 
                             max_len=60, 
                             start_chapter='cf', start_tag=None, 
                             leaf=False)
    classi = MultiParentAndChild(fdict='dictionary.csv', 
                             fmodel='cashflow_st_cashtype_pch_v2019-03-13.h5', 
                             max_len=60, 
                             start_chapter='cf', start_tag=None, 
                             leaf=True)
    classi.filter_model = fclassi
    classi.filter_id = 1
    
    fclassi.predict_all(structure)
    classi.predict_all(structure)
    
    ind = IndicatorRestated(name='mg_r_cash_operating_activities',
                            classifier=classi,
                            class_id=1)
    
    value, info = ind.calc(nums, 2018, adsh)
    
    