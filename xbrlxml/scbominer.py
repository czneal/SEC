import datetime as dt
import zipfile
import os
import pandas as pd
from typing import List, Tuple, IO, Iterator, TypeVar, Optional

import xbrlxml.scbo
from settings import Settings
from mysqlio.writers import MySQLWriter
from mysqlio.basicio import MySQLTable, retry_mysql_write


OwnerTuple = Tuple[int, dt.date, int, bool, bool, bool, bool, str, str]
OwnerColumns = [
    'issuer_cik',
    'period',
    'cik',
    'is_director',
    'is_officer',
    'is_other',
    'is_ten_percent',
    'officer_text',
    'other_text']


class SCBOWriter(MySQLWriter):
    def __init__(self):
        super().__init__()

        self.owners = MySQLTable('owners', self.con)

    def write(self, data: List[OwnerTuple]):
        row_list = [dict(zip(OwnerColumns, row)) for row in data]

        self.write_to_table(self.owners, row_list)
        self.flush()


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


_T = TypeVar('_T')


def default(value: Optional[_T], default_value: _T) -> _T:
    if value is None:
        return default_value
    return value


class SCBOMiner():
    def __init__(self):
        self.data: List[OwnerTuple] = []

    def mine_owners(self, cik: int):
        archive = Form4Archive(cik)

        for f in archive.enum():
            d = xbrlxml.scbo.open_document(f)
            if d.issuer.issuerCik != cik:
                continue

            for owner in d.reportingOwner:
                if owner.reportingOwnerRelationship.isDirector:
                    self.data.append(
                        (cik, d.periodOfReport, owner.reportingOwnerId.rptOwnerCik, default(
                            owner.reportingOwnerRelationship.isDirector, False), default(
                            owner.reportingOwnerRelationship.isOfficer, False), default(
                            owner.reportingOwnerRelationship.isOther, False), default(
                            owner.reportingOwnerRelationship.isTenPercentOwner, False), default(
                            owner.reportingOwnerRelationship.officerTitle, ''), default(
                            owner.reportingOwnerRelationship.otherText, '')))


if __name__ == '__main__':
    miner = SCBOMiner()
    writer = SCBOWriter()

    ciks = [1652044, 1750, 1800, 1788028]
    for cik in ciks:
        miner.mine_owners(cik)

    writer.write(miner.data)
