import json
import shutil
import os

from typing import Tuple, List
from numpy import random

import mysqlio.basicio as do
import mysqlio.indio
from utils import add_app_dir, add_root_dir, ProgressBar


class DataReader(mysqlio.indio.MySQLIndicatorFeeder):
    def fetch_record(self, adsh: str) -> Tuple[str, str]:
        data = self.fetch(
            'select record, file_link from sec_xbrl_forms where adsh=%(adsh)s', {
                'adsh': adsh})
        if data:
            return (data[0]['record'], data[0]['file_link'])
        else:
            return ('', '')

    def fetch_adshs(self, year: int, sample_size=300) -> List[str]:
        query = """
        select adsh from reports r, nasdaq n, stocks_index i
        where  r.cik = n.cik
            and i.index_name = 'sp5'
            and i.ticker = n.ticker
            and fin_year>=%(year)s;
        """
        data = self.fetch(query, {'year': year})
        if sample_size > len(data):
            sample_size = len(data)

        index = random.randint(0, len(data), size=sample_size)
        adshs = [data[i]['adsh'] for i in index]

        return adshs


def repair_adshs():
    dir_to_save = add_app_dir("xbrlxml/tests/res/backward/")

    for _, _, filenames in os.walk(dir_to_save):
        adshs = set([filename.split('.')[0]
                     for filename in filenames
                     if len(filename) >= 20])

    adshs = list(adshs)

    with open(os.path.join(dir_to_save, 'adshs.json'), 'w') as f:
        json.dump(adshs, fp=f, indent=2)


def main(append: bool, extra_adshs: List[str] = []):
    dir_to_save = add_app_dir("xbrlxml/tests/res/backward/")

    # clear dir
    if not append:
        if os.path.exists(dir_to_save):
            shutil.rmtree(dir_to_save, ignore_errors=True)
            while os.path.exists(dir_to_save):
                pass

    # create if not exist
    if not os.path.exists(dir_to_save):
        os.mkdir(dir_to_save)

    r = DataReader()
    if not extra_adshs:
        adshs = r.fetch_adshs(2013, sample_size=100)
    else:
        adshs = extra_adshs

    pb = ProgressBar()
    pb.start(len(adshs))

    for adsh in adshs[:]:
        facts = r.fetch_facts(adsh)
        record, file_link = r.fetch_record(adsh)

        prefix = os.path.join(dir_to_save, adsh)
        try:
            with open(prefix + '.facts', 'w') as f:
                f.write(json.dumps(facts, indent=2))
        except Exception:
            print(f'unable write .facts: {adsh}')
            adshs.remove(adsh)
            continue

        try:
            with open(prefix + '.record', 'w') as f:
                f.write(record)
        except Exception:
            print(f'unable write .record: {adsh}')
            adshs.remove(adsh)
            continue

        try:
            shutil.copy(add_root_dir(file_link),
                        os.path.join(dir_to_save, adsh + '.zip'))
        except Exception:
            print(f'unable write zip: {adsh}')
            adshs.remove(adsh)

        pb.measure()
        print('\r' + pb.message(), end='')
    print()

    try:
        repair_adshs()
    except Exception:
        print('unable save adshs')


if __name__ == '__main__':
    # main(append=True, extra_adshs=['0000882184-17-000103'])
    repair_adshs()
