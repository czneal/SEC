import datetime as dt
import zipfile
import os
import pandas as pd
from typing import List, Tuple, IO, Iterator, TypeVar, Optional, Any, Set

import xbrlxml.scbo
import xbrldown.scbodwn
import mpc
import logs
from abstractions import Worker, Writer
from settings import Settings
from mysqlio.writers import MySQLWriter
from mysqlio.basicio import MySQLTable
from utils import ProgressBar

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
NonDerTransColumns = [
    'adsh',
    'issuer',
    'period',
    'cik',
    'title',
    'trans_date',
    'trans_code',
    'equity_involved',
    'shares',
    'price',
    'acc_disp',
    'owner_nature',
    'post_shares',
    'post_values',
    'shares_notes',
    'price_notes'
]


class SCBOWriter(MySQLWriter):
    def __init__(self):
        super().__init__()

        self.owners = MySQLTable('owners', self.con)
        self.trans = MySQLTable('insider_trans', self.con)

    def write(self, obj: Tuple[List[OwnerTuple], List[List[Any]]]):
        row_list = [dict(zip(OwnerColumns, row)) for row in obj[0]]
        self.write_to_table(self.owners, row_list)

        row_list = [dict(zip(NonDerTransColumns, row)) for row in obj[1]]

        adshs = set([row['adsh'] for row in row_list])
        self.execute_in(
            "delete from insider_trans where adsh in (__in__)",
            adshs)

        self.write_to_table(self.trans, row_list)

        self.flush()


class SCBOPandasWriter(Writer):
    def __init__(self, filename: str):
        self.data: List[List[Any]] = []
        self.filename = filename

    def write(self, obj: List[List[Any]]):
        self.data.extend(obj)

    def flush(self):
        pd.DataFrame(
            self.data,
            columns=NonDerTransColumns).to_csv(
            self.filename)


class Form4Archive():
    def __init__(self, cik: int):
        zfilename = os.path.join(Settings.form4_dir(), f'{cik}.zip')
        if not os.path.exists(zfilename):
            raise FileNotFoundError(f'file {zfilename} not found')

        self.zf = zipfile.ZipFile(zfilename, 'r')
        self.filelist: List[str] = [info.filename for info in self.zf.filelist]

    def enum(self) -> Iterator[Tuple[IO, str]]:
        for filename in self.filelist:
            yield (self.zf.open(filename), filename)


_T = TypeVar('_T')


def default(value: Optional[_T], default_value: _T) -> _T:
    if value is None:
        return default_value
    return value


class SCBOMiner(Worker):
    def __init__(self, days_ago: int):
        self.reader = xbrldown.scbodwn.FormReader()
        self.days_ago = days_ago

    def feed(self, cik: int) -> Tuple[List[OwnerTuple],
                                      List[List[Any]]]:
        try:
            _, adshs = self.reader.fetch_form_links(
                cik, days_ago=self.days_ago)
            return (self.mine_owners(cik, adshs),
                    self.get_nonderivative_transactions(cik, adshs))
        except Exception:
            return ([], [])

    def flush(self):
        pass

    def mine_owners(self, cik: int, adshs: Set[str]) -> List[OwnerTuple]:
        archive = Form4Archive(cik)
        data: List[OwnerTuple] = []
        for f, filename in archive.enum():
            if filename[:20] not in adshs:
                continue

            try:
                d = xbrlxml.scbo.open_document(f)
            except Exception:
                logs.get_logger(__name__).error(
                    f'cik: {cik}, file: {filename} broken', exc_info=True)

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

    def get_nonderivative_transactions(
            self,
            cik: int,
            adshs: Set[str]) -> List[List[Any]]:
        archive = Form4Archive(cik)
        data: List[List[Any]] = []

        for f, filename in archive.enum():
            if filename[:20] not in adshs:
                continue

            try:
                d = xbrlxml.scbo.open_document(f)
            except Exception:
                logs.get_logger(__name__).error(
                    f'cik: {cik}, file: {filename} broken', exc_info=True)

            if d.issuer.issuerCik != cik:
                continue

            for t in d.nonDerivativeTable:
                row = [
                    filename[:20],
                    cik,
                    d.periodOfReport,
                    d.reportingOwner[0].reportingOwnerId.rptOwnerCik,
                    t.securityTitle,
                    t.transactionDate,
                    t.transactionCoding.transactionCode if t.transactionCoding is not None else None,
                    t.transactionCoding.equitySwapInvolved if t.transactionCoding is not None else None,
                    t.transactionAmounts.transactionShares,
                    t.transactionAmounts.transactionPricePerShare,
                    t.transactionAmounts.transactionAcquiredDisposedCode,
                    t.ownershipNature,
                    t.postTransactionAmounts.sharesOwnedFollowingTransaction,
                    t.postTransactionAmounts.valueOwnedFollowingTransaction,
                    '\n'.join(
                        [d.footnotes[fid]
                         for fid in t.transactionAmounts.shares_notes]),
                    '\n'.join([d.footnotes[fid] for fid in t.transactionAmounts.price_notes])]
                data.append(row)

        return data


def configure_writer() -> SCBOWriter:
    return SCBOWriter()


def configure_worker() -> SCBOMiner:
    return SCBOMiner(days_ago=365 * 7)


def parse_insiders_mpc(ciks: List[int],
                       handler_name: str,
                       level: int,
                       n_procs: int):

    manager = mpc.MpcManager(handler_name, level)

    manager.start(ciks,
                  configure_writer=configure_writer,
                  configure_worker=configure_worker,
                  n_procs=n_procs)


def parse_insiders(ciks: List[int], days_ago: int) -> None:
    miner = SCBOMiner(days_ago)
    writer = SCBOWriter()

    pb = ProgressBar()
    pb.start(len(ciks))

    for cik in ciks:
        owners, trans = miner.feed(cik)
        writer.write((owners, trans))

        pb.measure()
        print('\r' + pb.message(), end='')
    print()


def main():
    # ciks = [72971, 70858, 1067983, 19617, 40545]
    # parse_insiders(ciks, days_ago=60)
    reader = xbrldown.scbodwn.FormReader()
    ciks = reader.fetch_nasdaq_ciks()

    parse_insiders_mpc(ciks, 'file', logs.logging.INFO, 8)


if __name__ == '__main__':
    main()
