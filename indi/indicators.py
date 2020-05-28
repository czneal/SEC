from typing import Dict, Optional

from indi.modclass import Classifier
from indi.feeder import Feeder
from indi.types import Nums
from indi.indprocs import Indicator
from indi.indprocs import create as create_proc
from algos.calc import calc_indicator, calc_indicator_whole
from algos.scheme import Chapters


class IndicatorRestated(Indicator):
    def __init__(self,
                 name: str,
                 classifier: Classifier,
                 class_id: int,
                 feeder: Feeder):
        super().__init__(name)
        self.classifier = classifier
        self.class_id = class_id
        self.feeder = feeder

    def calc(self, nums: Nums, fy: int, s: Chapters) -> Optional[float]:
        try:
            start_chapter, start_node = self.feeder.find_start(s)
        except IndexError:
            return None

        pairs = self.feeder.filter(s)
        classes = self.classifier.predict(pairs)
        facts: Dict[str, float] = {}
        nums_fy = nums.get(fy, {})
        for ((_, c), _) in filter(
            lambda x: x[1] == self.class_id, zip(
                pairs, classes)):
            if c in nums_fy:
                facts[c] = nums_fy[c]

        if start_node is None:
            return calc_indicator_whole(start_chapter, facts)

        return calc_indicator(start_node, facts, used_tags=set())

    def description(self) -> str:
        return 'indicator:{0}\nclass id: {1}\n{2}'.format(
            self.name,
            self.class_id,
            self.classifier.description())


def create(ind_name: str,
           type_: str,
           model_name: str = '',
           class_id: int = 0,
           feeder_name: str = '',
           classifiers: Dict[str, Classifier] = {},
           feeders: Dict[str, Feeder] = {},) -> Indicator:
    if type_ == 'restated':
        if model_name not in classifiers:
            raise IndexError(f"model name: {model_name} doesn't defined")
        if feeder_name not in feeders:
            raise IndexError(f"feeder: {feeder_name} doesn't defined")

        return IndicatorRestated(name=ind_name,
                                 classifier=classifiers[model_name],
                                 class_id=class_id,
                                 feeder=feeders[feeder_name])
    if type_ == 'static':
        return create_proc(name=ind_name)
    if type_ == 'dynamic':
        return create_proc(name=ind_name)

    raise ValueError(f'bad indicator type: {type_}')


if __name__ == '__main__':
    pass
