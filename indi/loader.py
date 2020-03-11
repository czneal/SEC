# -*- coding: utf-8 -*-
import json
import os
from typing import Dict, Tuple, Optional, Any, List

import indi.modclass
from indi.modclass import Classifier
import indi.feeder
from indi.feeder import Feeder
import indi.indicators
from indi.indicators import Indicator
from indi.indpool import IndicatorsPool

from settings import Settings


def load_classifiers() -> List[Classifier]:
    with open(os.path.join(
            Settings.models_dir(),
            'classifiers.json')) as f:
        classifiers = json.load(f)

    cl_objects: List[Classifier] = []
    for model_id, sett in enumerate(classifiers):
        cl_objects.append(indi.modclass.create(
            sett['model'],
            model_id,
            sett['dict'],
            sett['pc'],
            sett['multi'],
            sett['max_len']))
    return cl_objects


def load_feeders(classifiers: Dict[str,
                                   Classifier]) -> Dict[str, Feeder]:
    with open(os.path.join(Settings.models_dir(), 'feeders.json')) as f:
        data = json.load(f)

    feeders: Dict[str, indi.feeder.Feeder] = {}
    for name, sett in data.items():
        classifier = classifiers.get(sett.get('model', ''), None)
        feeder = indi.feeder.create(sett['chapter'],
                                    sett['names'],
                                    cl=classifier,
                                    cl_id=sett.get('class_id', -1))
        feeders[name] = feeder

    return feeders


def load_indicators(classifiers: Dict[str, Classifier],
                    feeders: Dict[str, Feeder]) -> Dict[str, Indicator]:

    with open(os.path.join(Settings.models_dir(), 'indicators.json')) as f:
        data = json.load(f)

    indicators: Dict[str, Indicator] = {}
    for name, sett in data.items():
        ind = indi.indicators.create(
            ind_name=name,
            type_=sett['type'],
            model_name=sett.get('model_name', ''),
            class_id=sett.get('class_id', 0),
            feeder_name=sett.get('feeder', ''),
            classifiers=classifiers,
            feeders=feeders)
        indicators[name] = ind
    return indicators


def load() -> IndicatorsPool:
    classifiers = {cl.model_name: cl for cl in load_classifiers()}
    feeders = load_feeders(classifiers)
    indicators = load_indicators(classifiers, feeders)

    pool = IndicatorsPool(indicators)

    return pool


if __name__ == "__main__":
    pass
