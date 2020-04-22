import datetime

from typing import Tuple

import xbrlxml.dataminer
import xbrlxml.xbrlrss

from utils import add_root_dir


def read_report(adsh: str) -> Tuple[xbrlxml.dataminer.NumericDataMiner,
                                    xbrlxml.xbrlrss.FileRecord,
                                    bool]:

    en = xbrlxml.xbrlrss.MySQLEnumerator()
    en.set_filter_method(
        'explicit', after=datetime.date(
            2013, 1, 1), adsh=adsh)

    records = en.filing_records()
    if not records:
        raise ValueError(f'no such adsh: {adsh}')

    record, file_link = records[0]
    file_link = add_root_dir(file_link)

    dm = xbrlxml.dataminer.NumericDataMiner()
    r = dm.feed(record, file_link)

    return (dm, record, r)
