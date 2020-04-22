from typing import Dict, Any, List, Tuple, cast
from mysqlio.basicio import InternalError

import indi.indinfo as I
import indi.indcache as C

from indi.types import Nums, Facts
from algos.scheme import Chapters
from algos.xbrljson import loads
from utils import retry
from mysqlio.basicio import MySQLTable
from mysqlio.writers import MySQLWriter
from mysqlio.readers import MySQLReader


class IndicatorsWriter(MySQLWriter):
    def __init__(self):
        super().__init__()

        self.ind_proc = MySQLTable('ind_proc_info', self.con)
        self.ind_rest = MySQLTable('ind_rest_info', self.con)
        self.classifier_pairs = MySQLTable('ind_classified_pairs', self.con)
        self.indicators = MySQLTable('indicators', self.con)

    def write(self, inds: List[Dict[str, Any]]) -> None:
        self.write_to_table(self.indicators, inds)

    def write_ind_info(self,
                       ind_proc_info: List[I.IndProcInfo],
                       ind_rest_info: List[I.IndRestInfo],) -> None:

        self.write_to_table(self.ind_proc, ind_proc_info)
        self.write_to_table(self.ind_rest, ind_rest_info)

    def write_classified_pairs(self, pairs: List[Dict[str, Any]]) -> None:
        self.write_to_table(self.classifier_pairs, pairs)

    def truncate(self) -> None:
        truncate_tables = """
        truncate table ind_proc_info;
        truncate table ind_rest_info;
        truncate table ind_classified_pairs;
        truncate table indicators;
        """
        for result in self.cur.execute(truncate_tables, multi=True):
            pass

        self.flush()


def write_info(pool: I.IndicatorsPool) -> None:
    iw = IndicatorsWriter()

    ind_proc, ind_rest = I.ind_info(pool)

    iw.write_ind_info(ind_proc, ind_rest)
    iw.write_classified_pairs(I.cache_info())


class MySQLIndicatorFeeder(MySQLReader):
    def fetch_indicator_data(self, cik: int, fy: int, deep: int) \
            -> Tuple[Dict[int, Chapters], Dict[int, str]]:
        try:
            fy_adsh: Dict[int, str] = {}
            chapters: Dict[int, Chapters] = {}

            data = self.fetch(q_get_sorted_adsh, {
                'cik': cik, 'st': fy - deep + 1, 'fin': fy})

            for row in data:
                year = cast(int, row['fin_year'])
                if year in fy_adsh:
                    continue

                fy_adsh[year] = cast(str, row['adsh'])
                chapters[year] = loads(row['structure'])
        except Exception:
            return ({}, {})
        return (chapters, fy_adsh)

    def fetch_facts(self, adsh: str) -> Facts:
        facts: Facts = {}

        try:
            data = self.fetch(q_get_facts, {'adsh': adsh})
            for row in data:
                facts[row['version'] + ':' + row['tag']] = float(row['value'])

            return facts
        except Exception:
            return facts

    def fetch_nums(self, fy_adsh: Dict[int, str]) -> Nums:
        nums: Nums = {}
        for fy, adsh in fy_adsh.items():
            nums[fy] = self.fetch_facts(adsh)

        return nums

    def fetch_indicators(self, cik: int, fy: int, deep: int) \
            -> Dict[str, Dict[int, float]]:

        data = self.fetch(
            q_get_indicators, {
                'cik': cik, 'st': fy - deep + 1, 'fn': fy})

        retval: Dict[str, Dict[int, float]] = {}
        for row in data:
            retval.setdefault(row['name'], {})[row['fy']] = float(row['value'])

        return retval

    def fetch_ind_info(self) \
            -> Tuple[List[I.IndProcInfo], List[I.IndRestInfo]]:

        proc = self.fetch('select * from ind_proc_info;')
        rest = self.fetch('select * from ind_rest_info;')

        return (cast(List[I.IndProcInfo], proc),
                cast(List[I.IndRestInfo], rest))

    def fetch_classified_pairs(self) -> Dict[Tuple[str, str, int], int]:
        data = self.fetch("select * from ind_classified_pairs;")
        return {(cast(str, row['parent']),
                 cast(str, row['child']),
                 cast(int, row['model_id'])): cast(int, row['label'])
                for row in data}

    def fetch_snp500_ciks(self, fy: int) -> List[int]:
        data = self.fetch(q_get_snp500_ciks, {'fy': fy})
        return [cast(int, row['cik']) for row in data]


q_get_snp500_ciks = """
select r.cik from reports r, stocks_index i, nasdaq n
where fin_year = %(fy)s
	and r.cik = n.cik
    and i.ticker = n.ticker
    and index_name = 'sp5'
group by r.cik;
"""

q_get_indicators = """
select * from indicators
where cik = %(cik)s
    and fy between %(st)s and %(fn)s;
"""

q_get_sorted_adsh = """
select * from reports
where form in ('10-k', '10-k/a')
	and cik = %(cik)s
	and fin_year between %(st)s and %(fin)s
order by fin_year, file_date desc, adsh desc;
"""
q_get_facts = """
select tag, version, value
from mgnums
where adsh = %(adsh)s
    and value is not null;
"""

if __name__ == '__main__':
    iw = IndicatorsWriter()
    iw.truncate()
