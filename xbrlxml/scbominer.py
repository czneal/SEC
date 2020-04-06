import datetime as dt
import zipfile
import os
import pandas as pd
from typing import List, Tuple, IO, Iterator, TypeVar, Optional

import xbrlxml.scbo
import xbrldown.scbodwn
import mpc
import logs
from abstractions import Worker
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


class SCBOMiner(Worker):
    def __init__(self):
        pass

    def feed(self, cik: int) -> List[OwnerTuple]:
        try:
            return self.mine_owners(cik)
        except Exception:
            return []

    def flush(self):
        pass

    def mine_owners(self, cik: int) -> List[OwnerTuple]:
        archive = Form4Archive(cik)
        data: List[OwnerTuple] = []
        for f in archive.enum():
            try:
                d = xbrlxml.scbo.open_document(f)
            except Exception:
                logs.get_logger(__name__).error(
                    f'file: {f} broken', exc_info=True)

            if d.issuer.issuerCik != cik:
                continue

            for owner in d.reportingOwner:
                if owner.reportingOwnerRelationship.isDirector:
                    data.append(
                        (cik, d.periodOfReport, owner.reportingOwnerId.rptOwnerCik, default(
                            owner.reportingOwnerRelationship.isDirector, False), default(
                            owner.reportingOwnerRelationship.isOfficer, False), default(
                            owner.reportingOwnerRelationship.isOther, False), default(
                            owner.reportingOwnerRelationship.isTenPercentOwner, False), default(
                            owner.reportingOwnerRelationship.officerTitle, ''), default(
                            owner.reportingOwnerRelationship.otherText, '')))
        return data


def configure_writer() -> SCBOWriter:
    return SCBOWriter()


def configure_worker() -> SCBOMiner:
    return SCBOMiner()


def parse_owners_mpc(ciks: List[int],
                     handler_name: str, level: int, n_procs: int):
    manager = mpc.MpcManager(handler_name, level)

    manager.start(ciks, configure_writer=configure_writer,
                  configure_worker=configure_worker, n_procs=n_procs)


def parse_owners():
    miner = SCBOMiner()
    writer = SCBOWriter()

    for cik in ciks:
        data = miner.mine_owners(cik)
        writer.write(data)


if __name__ == '__main__':
    reader = xbrldown.scbodwn.FormReader()
    ciks = reader.fetch_nasdaq_ciks()

    parse_owners_mpc(ciks[:10], 'file', logs.logging.INFO, 8)
