from typing import List, Dict, Any

import indi.indinfo as info
import indi.types

from indi.indpool import IndicatorsPool
from indi.loader import load
from mysqlio.indio import MySQLIndicatorFeeder, IndicatorsWriter


def extract_indicators(nums: indi.types.Nums,
                       fy_adsh: Dict[int, str],
                       cik: int) -> List[Dict[str, Any]]:
    ind = []
    for fy, facts in nums.items():
        ind.extend([{'adsh': fy_adsh[fy],
                     'fy': fy,
                     'cik': cik,
                     'name': f,
                     'value': v}
                    for f, v in facts.items()
                    if f.startswith('mg_')])

    return ind


def main():
    ir = MySQLIndicatorFeeder()
    iw = IndicatorsWriter()

    pool = load()

    cik = 72971
    chapters, fy_adsh = ir.fetch_indicator_data(cik=cik, fy=2018, deep=5)
    nums = ir.fetch_nums(fy_adsh)

    nums = pool.calc(nums, chapters)
    inds = extract_indicators(nums, fy_adsh, cik)

    iw.write(inds)
    iw.write_classified_pairs(info.cache_info())
    iw.write_ind_info(*info.ind_info(pool))
    iw.flush()


if __name__ == '__main__':
    main()
