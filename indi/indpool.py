# -*- coding: utf-8 -*-
from typing import Dict, List

from indi.indicators import Indicator
from indi.types import Nums
from algos.scheme import Chapters


class IndicatorsPool():
    def __init__(self, indicators: Dict[str, Indicator]):
        self.indicators = indicators
        self.indicator_order: List[str] = []

        self._sort_indicators()

    def _sort_indicators(self):
        self.indicator_order = []
        total = -1
        while total < len(self.indicator_order):
            total = len(self.indicator_order)

            for ind_name, ind in self.indicators.items():
                if ind_name in self.indicator_order:
                    continue
                if ind.dependencies().issubset(self.indicator_order):
                    self.indicator_order.append(ind_name)

        if len(self.indicator_order) != len(self.indicators):
            sdiff = set(
                self.indicator_order).symmetric_difference(
                set(self.indicators.keys()))
            message = "impossible to order indicators: {0}".format(sdiff)
            for ind in sdiff:
                dp = self.indicators[ind].dependencies()
                dp = dp.difference(set(self.indicator_order))
                message += '\nindicator: {0}, dep: {1}'.format(ind, dp)
            raise Exception(message)

    def calc(self, nums: Nums, fy_structure: Dict[int, Chapters]) -> Nums:
        """
        calculate indicators for one set of Nums
        """
        years = sorted(fy_structure.keys())
        for fy in years:
            for ind_name in self.indicator_order:
                ind = self.indicators[ind_name]
                value = ind.calc(nums, fy, fy_structure[fy])
                if value:
                    nums.setdefault(fy, {})[ind_name] = value

        return nums


if __name__ == '__main__':
    pass
