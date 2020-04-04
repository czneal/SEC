import datetime as dt
import zipfile
import os
import pandas as pd
from typing import List, Tuple, IO, Iterator

import xbrlxml.scbo
from settings import Settings


class Form4Archive():
    def __init__(self, cik: int):
        zfilename = os.path.join(Settings.form4_dir(), f'{cik}.zip')
        if not os.path.exists(zfilename):
            raise FileNotFoundError(f'file {zfilename} not found')

        self.zf = zipfile.ZipFile(zfilename, 'r')
        self.filelist: List[str] = [info.filename for info in self.zf.filelist]

    def enum(self) -> Iterator[IO]:
        for filename in self.filelist:
            yield self.zf.open(filename)


class SCBOMiner():
    def __init__(self):
        self.data: List[List[Any]] = []

    def mine_directors(self, cik: int) -> List[Tuple[int, str, dt.date]]:
        archive = Form4Archive(cik)
        data: List[Tuple[int, str, dt.date]] = []

        for f in archive.enum():
            d = xbrlxml.scbo.open_document(f)
            if d.issuer.issuerCik != cik:
                continue

            for owner in d.reportingOwner:
                if owner.reportingOwnerRelationship.isDirector:
                    data.append((owner.reportingOwnerId.rptOwnerCik,
                                 owner.reportingOwnerId.rptOwnerName,
                                 d.periodOfReport))
        return data


if __name__ == '__main__':
    miner = SCBOMiner()
    directors = miner.mine_directors(1652044)

    d = pd.DataFrame(directors, columns=['cik', 'name', 'date'])
