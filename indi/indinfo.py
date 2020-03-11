import json
from typing import List, Tuple, Union, Dict

import indi.indcache
import indi.types as T
from indi.indpool import IndicatorsPool
from indi.indicators import Indicator, IndicatorRestated
from indi.indprocs import IndicatorProcedural


IndProcInfo = Dict[str, Union[str, int]]
IndRestInfo = Dict[str, Union[str, int]]
ClassifiedPair = Dict[str, Union[str, int]]


def cache_info() -> List[ClassifiedPair]:
    return [{'parent': e[0],
             'child': e[1],
             'model_id': e[2],
             'label': e[3]} for e in indi.indcache.to_list()]


def ind_proc_info(ind: IndicatorProcedural) -> IndProcInfo:
    return {'name': ind.name,
            'dp': json.dumps(list(ind.dp)),
            'deep': ind.deep}


def ind_rest_info(ind: IndicatorRestated) -> IndRestInfo:
    return {'name': ind.name,
            'model_name': ind.classifier.model_name,
            'model_id': ind.classifier.model_id,
            'class_id': ind.class_id,
            'chapter': ind.feeder.chapter,
            'nodes': json.dumps(ind.feeder.names)}


def ind_info(pool: IndicatorsPool) -> Tuple[List[IndProcInfo],
                                            List[IndRestInfo]]:
    """
    return (IndProcInfo, IndRestInfo)
    """
    proc_info = []
    rest_info = []
    for ind in pool.indicators.values():
        if isinstance(ind, IndicatorProcedural):
            proc_info.append(ind_proc_info(ind))
        elif isinstance(ind, IndicatorRestated):
            rest_info.append(ind_rest_info(ind))
        else:
            raise ValueError(f'unsupported Indicator type: {type(ind)}')

    return (proc_info, rest_info)


def indicators(nums: T.Nums) -> T.Nums:
    ind: T.Nums = {}
    for fy, facts in nums.items():
        ind[fy] = {k: v for k, v in facts.items() if k.startswith('mg_')}

    return ind
