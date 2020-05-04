import json
import shutil
import os

from typing import Tuple, List
from numpy import random

import mysqlio.basicio as do
import mysqlio.indio
from utils import add_app_dir, add_root_dir, ProgressBar


class DataReader(mysqlio.indio.MySQLIndicatorFeeder):
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

        prefix = os.path.join(dir_to_save, adsh)
        try:
            with open(prefix + '.facts', 'w') as f:
                f.write(json.dumps(facts, indent=2))
        except Exception:
            print(f'unable write .facts: {adsh}')
            adshs.remove(adsh)
            continue

        pb.measure()
        print('\r' + pb.message(), end='')
    print()


if __name__ == '__main__':
    main(append=False)
