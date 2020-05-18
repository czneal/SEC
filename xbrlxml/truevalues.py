# -*- coding: utf-8 -*-

import datetime as dt
from typing import Dict, Union, Mapping, Optional, cast, List
import json

import utils
import mysqlio.readers as readers
import mysqlio.writers as writers


class TrueValuesReader(readers.MySQLReader):
    def fetch_true_chapters(self) -> List[Dict[str, str]]:
        return cast(List[Dict[str, str]],
                    self.fetch("select * from true_chapters"))

    def fetch_true_periods(self) -> List[Dict[str, dt.date]]:
        return cast(List[Dict[str, dt.date]],
                    self.fetch("select * from true_periods"))


class Invert(writers.MySQLWriter):
    def invert_chapters(self):
        for adsh, chapters in TRUE_VALUES.chapters.items():
            if 'bs' in chapters:
                info = json.dumps({v: k for k, v in chapters.items()})
                self.cur.execute(
                    'update true_chapters set info=%(info)s where adsh=%(adsh)s', {
                        'adsh': adsh, 'info': info})
                self.con.commit()

    def write(self, obj):
        pass


class TrueValues():
    def __init__(self):
        r = TrueValuesReader()
        self.periods: Dict[str, dt.date] = {}
        self.chapters: Dict[str, Dict[str, str]] = {}

        for row in r.fetch_true_periods():
            self.periods[row['adsh']] = row['period']

        for row in r.fetch_true_chapters():
            self.chapters[row['adsh']] = json.loads(row['info'])

    def get_true_period(self, adsh: str) -> Union[dt.date, None]:
        return self.periods.get(adsh, None)

    def get_true_chapters(self, adsh: str) -> Optional[Dict[str, str]]:
        return self.chapters.get(adsh, None)


class TrueValuesTxt():
    def __init__(self, source_filenames: Mapping[str, str]):
        self.periods: Dict[str, dt.date] = {}
        self.chapters: Dict[str, Dict[str, str]] = {}

        with open(source_filenames['periods']) as f:
            for line in f.readlines():
                columns = line.replace('\n', '').replace('\r', '')
                self.periods[columns[:20]] = cast(
                    dt.date, utils.str2date(columns[21:]))

        with open(source_filenames['chapters']) as f:
            for line in f.readlines():
                columns = line.replace('\n', '').replace('\r', '')
                self.chapters[columns[:20]] = json.loads(columns[21:])

    def get_true_period(self, adsh: str) -> Union[dt.date, None]:
        return self.periods.get(adsh, None)

    def get_true_chapters(self, adsh: str) -> Optional[Dict[str, str]]:
        return self.chapters.get(adsh, None)


# TRUE_VALUES = TrueValues({'periods': (os.path.split(__file__)[0] +
#                                       '/truevalues_periods.csv'),
#                           'chapters': (os.path.split(__file__)[0] +
#                                        '/truevalues_chapters.csv')
#                           })
TRUE_VALUES = TrueValues()

if __name__ == "__main__":
    print(TRUE_VALUES.get_true_chapters('0001534504-19-000012'))

    # inv = Invert()
    # inv.invert_chapters()
