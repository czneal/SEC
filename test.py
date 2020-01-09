import re
import datetime as dt
import atexit
import string
import itertools
import pandas as pd
from collections import namedtuple
from typing import Dict, List, cast, Any, Tuple

from utils import str2date, ProgressBar
from urltools import fetch_with_delay
import mysqlio.basicio as do
import logs


class Share():
    def __init__(self):
        self.sclass: str = ''
        self.ticker: str = ''
        self.count: int = 0
        self.date: dt.date = dt.date(1990, 1, 1)

    def __str__(self) -> str:
        return (f'(sclass: {self.sclass}, ' +
                f'count: {self.count}, ' +
                f'ticker: {self.ticker}, ' +
                f'date: {self.date})')

    def __repr__(self) -> str:
        return self.__str__()


class MySQLReader():
    def __init__(self):
        self.con = do.open_connection()
        self.cur = self.con.cursor(dictionary=True)
        atexit.register(self.close)

    def close(self) -> None:
        try:
            self.con.close()
        except Exception:
            pass


class MySQLShares(MySQLReader):
    def fetch_sec_shares(self, adsh: str) -> List[Dict[str, Any]]:
        try:
            self.cur.execute(q_get_shares_sec_adsh, {'adsh': adsh})
            data = self.cur.fetchall()
            return cast(List[Dict[str, Any]], data)
        except Exception:
            return []

    def fetch_stocks_shares(self, 
                            cik: int,
                            around: dt.date) -> List[Dict[str, Any]]:
        try:
            self.cur.execute(q_get_shares_stocks_cik, 
                            {'cik': cik,
                             'date': around})
            data = self.cur.fetchall()
            return cast(List[Dict[str, Any]], data)
        except Exception:
            return []

    def fetch_tickers(self, cik: int) -> List[str]:
        try:
            self.cur.execute(q_get_tickers, {'cik': cik})
            data = self.cur.fetchall()
            return [cast(str, r['ticker']) for r in data]
        except Exception:
            return []


class MySQLReports(MySQLReader):
    def find_latest(self, cik: int) -> str:
        try:
            self.cur.execute(q_find_latest_report, (cik,))
            result = self.cur.fetchall()
            if result:
                return cast(str, result[0]['adsh'])
            else:
                return ''
        except Exception:
            return ''


def get_shares(cik: int,
               around: dt.date,
               shares_reader: MySQLShares,
               reports_reader: MySQLReports) -> Dict[str, List[Share]]:
    shares = get_stocks_shares(cik, around, shares_reader, reports_reader)
    if shares:
        return shares

    return get_sec_shares(cik, shares_reader, reports_reader)


def get_stocks_shares(cik: int,
                      around: dt.date,
                      shares_reader: MySQLShares,
                      reports_reader: MySQLReports) -> Dict[str, List[Share]]:
    
    data = shares_reader.fetch_stocks_shares(cik, around)
    return process_stocks_shares(data, around)


def get_sec_shares(cik: int,
                   shares_reader: MySQLShares,
                   reports_reader: MySQLReports) -> Dict[str, List[Share]]:
    adsh = reports_reader.find_latest(cik)
    data = shares_reader.fetch_sec_shares(adsh)

    return process_sec_shares(data)


def filter_shares(shares: List[Share]) -> List[Share]:
    if not shares:
        return []

    shares = sorted(shares, key=lambda x: x.date, reverse=True)
    edate = shares[0].date
    return [share for share in filter(lambda x: x.date == edate, shares)]


def process_stocks_shares(data: List[Dict[str, Any]],
                          around: dt.date) -> Dict[str, List[Share]]:
    priority = ['stock.com', 'share.com', 'share.adr', 'share.ord', 'share',
                'share.adr.com', 'unit.com', 'unit', 'fund']
    shares: Dict[str, List[Share]] = {}
    share_class = re.compile(r'[\w, \.]+class(\w{1})\w*', re.IGNORECASE)
    marker = re.compile(r'^stock|^share', re.IGNORECASE)
    
    for row in data:
        ttype = str(row['ttype'])
        if not marker.match(ttype):
            continue
        if 'pref' in ttype or 'adr' in ttype:
            continue

        share = Share()
        share.date = cast(dt.date, str2date(row['trade_date']))
        share.ticker = row['ticker']
        share.count = int(row['shares'])
        
        f = share_class.findall(ttype)
        if f:
            index = f[0].upper()
        else:
            index = ''
        
        share.sclass = 'class' + index
        if index not in shares:
            shares[index] = [share]
        else:
            shares[index].append(share)
        
    if not shares and len(data) == 1:
        share = Share()
        ttype = data[0]['ttype']        
        share.date = cast(dt.date, str2date(row['trade_date']))
        share.ticker = row['ticker']
        share.count = int(row['shares'])

        f = share_class.findall(ttype)
        if f:
            index = f[0].upper()
        else:
            index = ''
        share.sclass = 'class' + index
        shares[index] = [share]

    return shares

def find_ticker(tickers: List[str], shares: List[Share]) -> None:
    if len(tickers) == 0 or len(shares) == 0:
        return

    if len(tickers) == 1 and len(shares) == 1:
        shares[0].ticker = tickers[0]
        return
    
    if len(tickers) == 1 and len(shares) > 1:
        for share in shares:
            share.ticker = tickers[0]
        return

    pref = process_tickers(tickers)
    for share in shares:
        if share.sclass == 'class':
            if pref['pure'] != '':
                share.ticker = pref['pure']
        else:
            share.ticker = pref.get(share.sclass[5:6], '')

def process_tickers(tickers: List[str]) -> Dict[str, str]:
    tickers = [t.upper() for t in tickers]
    ret: Dict[str, str] = {}

    pure = [t for t in tickers if re.match(r'^\w{1,4}$', t, re.IGNORECASE)]
    if len(pure) == 1:
        ret['pure'] = pure[0]
    else:
        ret['pure'] = ''

    pref = find_com_pref(tickers)

    letters = set(list(string.ascii_uppercase))
    for ticker in tickers:
        if ticker == ret['pure']:
            continue

        parts = ticker.replace('.', '^').split('^')
        if len(parts) > 1:
            if parts[-1] in letters:
                ret[parts[-1]] = ticker
            else:
                print('process_tickers 1: ', ret['pure'], ticker)
        else:
            index = ticker[len(pref):]
            if index in letters:
                ret[index] = ticker
            else:
                print('process_tickers 2: ', ret['pure'], ticker)
    
    return ret


def find_com_pref(strs: List[str]) -> str:
    letter_groups, longest_pre = zip(*strs), ""
    # print(letter_groups, longest_pre)
    # [('f', 'f', 'f'), ('l', 'l', 'l'), ('o', 'o', 'i'), ('w', 'w', 'g')] 
    for letter_group in letter_groups:
        if len(set(letter_group)) > 1: break
        longest_pre += letter_group[0]

    return longest_pre

def process_sec_shares(data: List[Dict[str, Any]]) -> Dict[str, List[Share]]:
    shares: Dict[str, List[Share]] = {}
    share_class = re.compile(r'.+class(\w{1}).*', re.IGNORECASE)

    for row in data:
        count = int(row['value'])
        if count == 0:
            continue

        share = Share()
        share.date = cast(dt.date, str2date(row['edate'], 'ymd'))
        share.count = count
        member = row['member'] if row['member'] else ''
        
        f = share_class.findall(member)
        if f:
            index = f[0].upper()
        else:
            index = ''
        
        share.sclass = 'class' + index
        if index in shares:
            shares[index].append(share)
        else:
            shares[index] = [share]

    if '' in shares and len(shares) > 1:
        summa = sum([sum([e.count for e in v]) for k, v in shares.items() 
                                               if k != ''])
    else:
        return shares

    if summa >= shares[''][0].count:
        shares.pop('')    
        return shares
    else:
        # print(f'summa less than none member: {adsh}')
        return {'': shares['']}

def find_company_names(key_words: List[str], add_words: List[str]) -> List[Tuple[int, str]]:
    key_words = [w.lower() for w in key_words]
    add_words = [w.lower() for w in add_words]

    chunk = 100
    start = 0
    search = ''
    companies: List[Tuple[int, str]] = []

    for words in itertools.permutations(key_words):
        search = ' '.join(words)
        while True:
            url = (f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&" + 
                  f"company={search}&owner=exclude&match=&start={start}&count={chunk}&hidefilings=0")
            body = fetch_with_delay(url)
            if not body:
                break
            
            try:
                tables = pd.read_html(body)
            except Exception:
                break

            if not tables:
                break
            table = tables[0]
            if table.shape[0] == 0:
                break
            
            for index, row in table.iterrows():
                company = str(row['Company']).lower()
                if sum([(w in company) for w in add_words]) == len(add_words):
                    companies.append((int(row['CIK']), company))
                    
            start += chunk
    
    return companies
        

q_find_latest_report = """
select adsh from reports where cik=%s
order by file_date desc, adsh desc
limit 1;
"""

q_get_shares_sec_adsh = """
select s.* from sec_shares s,
(
	select ifnull(member, '') as member, max(edate) as edate 
    from sec_shares
	where adsh = %(adsh)s
		and name = 'dei:EntityCommonStockSharesOutstanding'
	group by member
) ss
where s.edate = ss.edate
	and ifnull(s.member, '') = ss.member
	and s.adsh = %(adsh)s
	and s.name = 'dei:EntityCommonStockSharesOutstanding';
"""

q_get_shares_stocks_cik = """
select s.*, ss.ttype from stocks_shares s,
(
	select s.ticker, ss.ttype, max(s.trade_date) as trade_date
    from stocks_shares s,
    (
		select s.ticker, n.ttype, min(abs(s.trade_date - date(%(date)s))) as mn
		from nasdaq n, stocks_shares s
		where s.ticker = n.ticker
			and s.shares > 0
			and n.cik = %(cik)s
		group by s.ticker
	) ss
    where ss.ticker = s.ticker
		and abs(s.trade_date - date(%(date)s)) = mn
    group by ss.ticker
) ss
where s.ticker = ss.ticker
	and s.trade_date = ss.trade_date;
"""

q_get_tickers = """
select ticker from nasdaq
where cik = %(cik)s;
"""

if __name__ == '__main__':
    # print(find_latest_report(790526))

    # no classes shares
    # print(get_shares_sec_adsh('0001628280-19-003087'))

    # summa equal none returns two share classes
    # print(get_shares_sec_adsh('0000009346-19-000012'))

    # several dates for classes shares
    # print(get_shares_sec_adsh('0001193125-13-116643'))

    # several none member shares
    # print(get_shares_sec_adsh('0001387131-13-000570'))

    # summa less '0001193125-13-096150'
    # summa more '0001144204-13-017209'
    
    # logs.configure('file', level=logs.logging.INFO)
    # logger = logs.get_logger()
    
    # query_1 = 'select cik from nasdaq where cik is not null group by cik'
    # with do.OpenConnection() as con:
    #     cur = con.cursor()
    #     cur.execute(query_1)
    #     ciks = [cast(int, cik) for (cik,) in cur.fetchall()]

    # shares_reader = MySQLShares()
    # reports_reader = MySQLReports()

    # # ciks = [1565025]
    # pb = ProgressBar()
    # pb.start(len(ciks))

    # for cik in ciks:
    #     tickers = shares_reader.fetch_tickers(cik) 
    #     sec_shares = get_sec_shares(cik, shares_reader, reports_reader)
    #     stocks_shares = get_stocks_shares(cik, dt.date(2019, 12, 10),
    #                             shares_reader, reports_reader)

    #     bad = False
    #     for v in sec_shares.values():
    #         if len(v) > 1:
    #             bad = True
    #             break
    #     for v in stocks_shares.values():
    #         if len(v) > 1:
    #             bad = True
    #             break
    #     if bad or len(stocks_shares)==0:
    #         logger.set_state(state={'state': str(cik)})
    #         logger.info(str(tickers))
    #         logger.info(str(sec_shares))
    #         logger.info(str(stocks_shares))
    #         logger.revoke_state()
            
    #     pb.measure()
    #     print('\r' + pb.message(), end='')
    # print()

    # shares_reader.close()
    # reports_reader.close()

    #*****************************

    # shares_reader = MySQLShares()
    # reports_reader = MySQLReports()
    # ciks = [1029800, 947484, 1067983, 7789, 5272, 1652044, 70858]
    # for cik in ciks[:]:    
    #     tickers = shares_reader.fetch_tickers(cik) 
    #     sec_shares = get_sec_shares(cik, shares_reader, reports_reader)
    #     stocks_shares = get_stocks_shares(cik, dt.date(2019, 12, 10),
    #                             shares_reader, reports_reader)

    #     bad = False
    #     for v in sec_shares.values():
    #         if len(v) > 1:
    #             bad = True
    #     for v in stocks_shares.values():
    #         if len(v) > 1:
    #             bad = True
    #     if bad:
    #         print(tickers)
    #         print(sec_shares)
    #         print(stocks_shares)


    # shares_reader.close()
    # reports_reader.close()

    'Invesco BLDRS Asia 50 ADR Index Fund'
    'RiverNorth Opportunities Fund, Inc.'
    'RiverNorth Opportunistic Municipal Income Fund, Inc.'
    'Merrill Lynch & Co., Inc.'
    import json
    with open('outputs/merrill.json', 'w') as f:
        json.dump(find_company_names(['Merrill', 'Lynch'], []), f)