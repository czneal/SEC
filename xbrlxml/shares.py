import re
import datetime as dt
import string
from typing import Dict, List, cast, Any, Tuple, TypeVar

from mysqlio.readers import MySQLShares
from utils import str2date


class Share():
    def __init__(self,
                 sclass: str = '', ticker: str = '',
                 count: int = 0, date: dt.date = dt.date(1990, 1, 1),
                 member: str = ''):
        self.sclass: str = sclass
        self.ticker: str = ticker
        self.count: int = count
        self.date: dt.date = date
        self.member: str = member

    def __str__(self) -> str:
        return (f'{{"sclass": "{self.sclass}", ' +
                f'"count": "{self.count}", ' +
                f'"ticker": "{self.ticker}", ' +
                f'"date": "{self.date}", ' +
                f'"member": "{self.member}"}}')

    def __repr__(self) -> str:
        return self.__str__()


_CT = TypeVar('_CT')


def check_shares(shares: Dict[str, List[_CT]]) -> bool:
    for _, v in shares.items():
        if len(v) > 1:
            return False
    return len(shares) > 0


def find_share_class(ttype: str) -> str:
    share_class = re.compile(r'[\w, \.]+class(\w{1})\w*', re.IGNORECASE)
    f = share_class.findall(ttype)
    if f:
        return cast(str, f[0].upper())

    return ''


def find_com_pref(strs: List[str]) -> str:
    letter_groups, longest_pre = zip(*strs), ""
    # print(letter_groups, longest_pre)
    # [('f', 'f', 'f'), ('l', 'l', 'l'), ('o', 'o', 'i'), ('w', 'w', 'g')]
    for letter_group in letter_groups:
        if len(set(letter_group)) > 1:
            break
        longest_pre += letter_group[0]

    return longest_pre


def process_tickers(tickers: List[Tuple[str, str]]) -> Dict[str, List[str]]:
    if len(tickers) == 1:
        index = find_share_class(tickers[0][1])
        return {index: [tickers[0][0]]}

    tickers = [(t.upper(), ttype) for (t, ttype) in tickers]
    ret: Dict[str, List[str]] = {}

    marker = re.compile(r'^stock|^share', re.IGNORECASE)

    for ticker, ttype in tickers:
        if not marker.match(ttype):
            continue
        if 'pref' in ttype or 'adr' in ttype:
            continue

        index = find_share_class(ttype)
        ret.setdefault(index, []).append(ticker)

    if check_shares(ret):
        return ret

    ret: Dict[str, List[str]] = {}
    pure = [
        t for (
            t,
            _) in tickers if re.match(
            r'^\w{1,4}$',
            t,
            re.IGNORECASE)]
    if len(pure) == 1:
        ret[''] = [pure[0]]

    pref = find_com_pref([t for t, _ in tickers])

    letters = set(list(string.ascii_uppercase))
    for ticker, _ in tickers:
        if ticker in ret.get('', []):
            continue

        parts = ticker.replace('.', '^').split('^')
        if len(parts) > 1:
            if parts[-1] in letters:
                ret.setdefault(parts[-1], []).append(ticker)
            else:
                ret.setdefault('', []).append(ticker)
        else:
            index = ticker[len(pref):]
            if index in letters:
                ret.setdefault(index, []).append(ticker)
            else:
                ret.setdefault('', []).append(ticker)

    return ret


def process_sec_shares(data: List[Dict[str, Any]]) -> Dict[str, List[Share]]:
    shares: Dict[str, List[Share]] = {}
    share_class = re.compile(r'.+Class([A-Z]{1}[0-9]{0,1}).*Member')

    for row in data:
        count = int(row['value'])
        if count == 0:
            continue

        share = Share(date=cast(dt.date, str2date(row['edate'], 'ymd')),
                      count=count,
                      member=row['member'])

        if share.member == '':
            shares.setdefault('null', []).append(share)
            continue

        f = share_class.findall(share.member)
        if f:
            index = f[0].upper()
        else:
            index = ''

        share.sclass = index
        shares.setdefault(index, []).append(share)

    if 'null' in shares:
        if len(shares) > 1:
            summa = sum([sum([e.count for e in v]) for k, v in shares.items()
                         if k != 'null'])
            if summa >= shares['null'][0].count:
                shares.pop('null')
            else:
                shares = {'': shares['null']}
        else:
            shares = {'': shares['null']}

    return shares


def predict_ticker(share: Share, cik: int,
                   shares_reader: MySQLShares) -> str:

    possible = shares_reader.fetch_possible_ticker(
        cik, share.member)
    if len(possible) == 1:
        return possible[0][1]

    return ''


def join_sec_stocks_tickers(
        shares: Dict[str, List[Share]],
        tickers: Dict[str, List[str]],
        cik: int,
        shares_reader: MySQLShares) -> bool:
    full_join = True
    if len(shares) == 1 and len(tickers) == 1:
        for k1, k2 in zip(shares, tickers):
            if len(shares[k1]) == 1 and len(tickers[k2]) == 1:
                shares[k1][0].ticker = tickers[k2][0]
                return full_join

    for letter, share_list in shares.items():
        if letter in tickers:
            if len(share_list) == 1 and len(tickers[letter]) == 1:
                share_list[0].ticker = tickers[letter][0]
            else:
                any_ticker = False
                for share in share_list:
                    share.ticker = predict_ticker(share, cik, shares_reader)
                    if share.ticker != '':
                        any_ticker = True
                if len(tickers[letter]) > 0 and not any_ticker:
                    full_join = False

    return full_join


def main():
    # cik = 1058290
    # adsh = '0001058290-20-000008'
    r = MySQLShares()

    sec_shares = process_sec_shares(r.fetch_sec_shares('0001058290-20-000008'))
    tickers = process_tickers(r.fetch_tickers(1058290))

    join_sec_stocks_tickers(sec_shares, tickers, 1058290, r)

    print(sec_shares)


if __name__ == '__main__':
    pass
