import json
import shutil

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


def main():
    dir_to_save = add_app_dir("xbrlxml/tests/res/backward/")
    if shutil.os.path.exists(dir_to_save):
        shutil.rmtree(dir_to_save, ignore_errors=True)
        while shutil.os.path.exists(dir_to_save):
            pass

    if not shutil.os.path.exists(dir_to_save):
        shutil.os.mkdir(dir_to_save)

    r = DataReader()
    adshs = r.fetch_adshs(2013, sample_size=100)
    adshs.append('0001564590-18-002832')

    pb = ProgressBar()
    pb.start(len(adshs))

    for adsh in adshs[:]:
        facts = r.fetch_facts(adsh)
        record, file_link = r.fetch_record(adsh)

        prefix = shutil.os.path.join(dir_to_save, adsh)
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
                        shutil.os.path.join(dir_to_save, adsh + '.zip'))
        except Exception:
            print(f'unable write zip: {adsh}')
            adshs.remove(adsh)

        pb.measure()
        print('\r' + pb.message(), end='')
    print()

    try:
        with open(shutil.os.path.join(dir_to_save, 'adshs.json'), 'w') as f:
            f.write(json.dumps(adshs))
    except Exception:
        print('unable save adshs')


if __name__ == '__main__':
    main()
