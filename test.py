import re
import datetime as dt
import atexit
import string
import itertools
import pandas as pd
from collections import namedtuple
from typing import Dict, List, cast, Any, Tuple, TypeVar

from utils import str2date, ProgressBar
from urltools import fetch_with_delay
import mysqlio.basicio as do
import logs


class Share():
    def __init__(self, 
                 sclass: str='', ticker: str='',
                 count: int =0, date: dt.date=dt.date(1990, 1, 1),
                 member: str=''):
        self.sclass: str = sclass
        self.ticker: str = ticker
        self.count: int = count
        self.date: dt.date = date
        self.member: str = member

    def __str__(self) -> str:
        return (f'(sclass: {self.sclass}, ' +
                f'count: {self.count}, ' +
                f'ticker: {self.ticker}, ' +
                f'date: {self.date}, ' +
                f'member: {self.member})')

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
    
    def fetch_tickers(self, cik: int) -> List[Tuple[str, str]]:
        try:
            self.cur.execute(q_get_tickers, {'cik': cik})
            data = self.cur.fetchall()
            return [(cast(str, r['ticker']), 
                    (cast(str, r['ttype']))) for r in data]
        except Exception:
            return []

    def fetch_nasdaq_ciks(self) -> List[int]:
        try:
            self.cur.execute(q_get_nasdaq_ciks)
            data = self.cur.fetchall()
            return [cast(int, r['cik']) for r in data]
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
    
    def fetch_adshs(self, cik: int) -> List[str]:
        try:
            self.cur.execute(q_get_adshs, {'cik': cik})
            return [cast(str, r['adsh']) for r in self.cur.fetchall()]
        except Exception:
            return []

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

_CT = TypeVar('_CT')

def check_shares(shares: Dict[str, List[_CT]]) -> bool:
    for k, v in shares.items():
        if len(v) > 1:
            return False
    return (len(shares) > 0)

def find_share_class(ttype: str) -> str:
    share_class = re.compile(r'[\w, \.]+class(\w{1})\w*', re.IGNORECASE)
    f = share_class.findall(ttype)
    if f:
        return cast(str, f[0].upper())
    else:
        return ''

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
    pure = [t for (t, _) in tickers if re.match(r'^\w{1,4}$', t, re.IGNORECASE)]
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

def join_sec_stocks_tickers(shares: Dict[str, List[Share]], tickers: Dict[str, List[str]]) -> bool:
    full_join = True
    for letter, share_list in shares.items():
        if letter in tickers:
            if len(share_list) == 1 and len(tickers[letter]) == 1:
                share_list[0].ticker = tickers[letter][0]
            else:
                full_join = False
    
    return full_join

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
select * from sec_shares
where (member like '%class%'
	or member like '%common%'
    or member like '%preferred%'
    or member like '%stock%'
    or member = '')
    and adsh = %(adsh)s;
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
select ticker, ttype from nasdaq
where cik = %(cik)s;
"""

q_get_adshs = """
select adsh from reports where cik = %(cik)s;
"""

q_get_nasdaq_ciks = """
select cik from nasdaq where cik is not null group by cik
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
    true_shares = {'0000318306-13-000004': {'us-gaap:CommonStockMember': 'AACP'},
                   '0000318306-14-000006': {'us-gaap:CommonStockMember': 'AACP'},
                   '0001539894-14-000007': {'us-gaap:CommonStockMember': 'AFH'},
                   '0001539894-15-000005': {'us-gaap:CommonStockMember': 'AFH'},
                   '0001539894-16-000050': {'us-gaap:CommonStockMember': 'AFH'},
                   '0001539894-17-000009': {'us-gaap:CommonStockMember': 'AFH'},
                   '0001193125-13-084136': {'us-gaap:CommonStockMember': 'ALSN'},
                   '0001193125-14-063895': {'us-gaap:CommonStockMember': 'ALSN'},
                   '0001047469-15-001510': {'us-gaap:CommonClassAMember': 'AMH'},                   
                   '0001193125-14-116022': {'us-gaap:CommonClassAMember': 'AMH'},
                   '0000946673-16-000014': {'banr:VotingCommonStockMember': 'BANR'},
                   '0000946673-17-000003': {'banr:VotingCommonStockMember': 'BANR'},
                   '0000946673-18-000009': {'banr:VotingCommonStockMember': 'BANR'},
                   '0000946673-19-000006': {'banr:VotingCommonStockMember': 'BANR'},
                   '0001564590-16-014218': {'bsm:CommonUnitsMember': 'BSM'},
                   '0001628280-17-002039': {'bsm:CommonUnitsMember': 'BSM'},
                   '0001628280-18-002529': {'bsm:CommonUnitsMember': 'BSM'},
                   '0001628280-19-001941': {'bsm:CommonUnitsMember': 'BSM'},
                   '0001047469-14-003251': {'byfc:VotingCommonStockMember': 'BYFC'},
                   '0001047469-15-002892': {'byfc:VotingCommonStockMember': 'BYFC'},
                   '0001047469-16-011660': {'byfc:VotingCommonStockMember': 'BYFC'},
                   '0001047469-17-002044': {'byfc:VotingCommonStockMember': 'BYFC'},
                   '0001047469-18-002146': {'byfc:VotingCommonStockMember': 'BYFC'},
                   '0001047469-19-001784': {'byfc:VotingCommonStockMember': 'BYFC'},
                   '0001136352-15-000003': {'': 'CEQP'},
                   '0001136352-16-000018': {'': 'CEQP'},
                   '0001136352-17-000003': {'': 'CEQP'},
                   '0001136352-18-000004': {'': 'CEQP'},
                   '0001136352-19-000003': {'': 'CEQP'},
                   '0001445305-14-000767': {'': 'CEQP'},
                   '0001445305-14-000848': {'': 'CEQP'},
                   '0001564590-19-008063': {'us-gaap:NonvotingCommonStockMember': 'CSTR'},
                   '0000068622-15-000003': {'': ''},
                   '0000068622-16-000013': {'': ''},
                   '0000068622-17-000003': {'': ''},
                   '0000068622-18-000004': {'': ''},
                   '0000068622-19-000003': {'': ''},
                   '0001047469-13-002605': {'': ''},
                   '0001445305-14-000932': {'': ''},
                   '0001628280-15-001193': {'cubi:VotingCommonStockMember': 'CUBI'},
                   '0001193125-13-112444': {'cubi:VotingCommonStockMember': 'CUBI'},
                   '0001193125-14-095432': {'cubi:VotingCommonStockMember': 'CUBI'},
                   '0001512077-14-000002': {'us-gaap:CommonStockMember': 'ENT'},
                   '0001363829-17-000017': {'esgr:VotingCommonStockMember': 'ESGR'},
                   '0001363829-18-000026': {'esgr:VotingCommonStockMember': 'ESGR'},
                   '0001363829-19-000023': {'esgr:VotingCommonStockMember': 'ESGR'},
                   '0001144204-13-068469': {'': 'EV'},
                   '0001144204-14-075076': {'': 'EV'},
                   '0001144204-15-071650': {'': 'EV'},
                   '0001144204-16-140665': {'': 'EV'},
                   '0001144204-17-064546': {'': 'EV'},
                   '0001144204-18-066592': {'': 'EV'},
                   '0001331875-16-000111': {'fnf:FidelityNationalFinancialGroupCommonStock': 'FNF'},
                   '0001331875-17-000031': {'fnf:FidelityNationalFinancialGroupCommonStock': 'FNF'},
                   '0001047469-13-001251': {'': 'FVE'},
                   '0001047469-14-007708': {'': 'FVE'},
                   '0001104659-14-027931': {'': 'FVE'},
                   '0001159281-17-000010': {'': 'FVE'},
                   '0001159281-18-000007': {'': 'FVE'},
                   '0001159281-19-000009': {'': 'FVE'},
                   '0001558370-15-000369': {'': 'FVE'},
                   '0001558370-16-003783': {'': 'FVE'},
                   '0001144204-13-015324': {'us-gaap:CommonStockMember': 'GSAT'},
                   '0001144204-14-014640': {'us-gaap:CommonStockMember': 'GSAT'},
                   '0001366868-15-000011': {'us-gaap:CommonStockMember': 'GSAT'},
                   '0001366868-16-000113': {'us-gaap:CommonStockMember': 'GSAT'},
                   '0001366868-17-000019': {'us-gaap:CommonStockMember': 'GSAT'},
                   '0001558370-19-002908': {'jagx:CommonStockVotingMember': 'JAGX'},
                   '0001011570-15-000008': {'us-gaap:CommonStockMember': 'KNL'},
                   '0001011570-16-000063': {'us-gaap:CommonStockMember': 'KNL'},
                   '0001011570-18-000012': {'us-gaap:CommonStockMember': 'KNL'},
                   '0001011570-19-000009': {'us-gaap:CommonStockMember': 'KNL'},
                   '0001445305-14-000826': {'us-gaap:CommonStockMember': 'KNL'},
                   '0001437749-16-028431': {'us-gaap:CommonStockMember': 'LMST'},
                   '0001437749-17-003431': {'us-gaap:CommonStockMember': 'LMST'},
                   '0001437749-18-003527': {'us-gaap:CommonStockMember': 'LMST'},
                   '0001437749-19-004472': {'us-gaap:CommonStockMember': 'LMST'},
                   '0001564590-19-004884': {'lob:VotingCommonStockMember': 'LOB'},
                   '0001144204-13-012427': {'lorl:VotingCommonStockMember': 'LORL'},
                   '0001144204-14-012911': {'lorl:VotingCommonStockMember': 'LORL'},
                   '0001144204-15-013290': {'lorl:VotingCommonStockMember': 'LORL'},
                   '0001144204-16-084790': {'lorl:VotingCommonStockMember': 'LORL'},
                   '0001144204-17-013033': {'lorl:VotingCommonStockMember': 'LORL'},
                   '0001144204-18-014926': {'lorl:VotingCommonStockMember': 'LORL'},
                   '0001558370-19-002187': {'lorl:VotingCommonStockMember': 'LORL'},
                   '0000065011-13-000104': {'us-gaap:CommonStockMember': 'MDP'},
                   '0000063754-13-000002': {'us-gaap:NonvotingCommonStockMember': 'MKC',
                                            'us-gaap:CommonStockMember': 'MKC.V'},
                   '0000063754-14-000008': {'us-gaap:NonvotingCommonStockMember': 'MKC',
                                            'us-gaap:CommonStockMember': 'MKC.V'},
                   '0000063754-15-000013': {'us-gaap:NonvotingCommonStockMember': 'MKC',
                                            'us-gaap:CommonStockMember': 'MKC.V'},
                   '0000063754-16-000063': {'us-gaap:NonvotingCommonStockMember': 'MKC',
                                            'us-gaap:CommonStockMember': 'MKC.V'},
                   '0000063754-17-000014': {'us-gaap:NonvotingCommonStockMember': 'MKC',
                                            'us-gaap:CommonStockMember': 'MKC.V'},
                   '0000063754-18-000008': {'us-gaap:NonvotingCommonStockMember': 'MKC',
                                            'us-gaap:CommonStockMember': 'MKC.V'},
                   '0000063754-19-000017': {'us-gaap:NonvotingCommonStockMember': 'MKC',
                                            'us-gaap:CommonStockMember': 'MKC.V'},                   
                   '0001439095-19-000012': {'us-gaap:CommonStockMember': 'MRC'},
                   '0001047469-14-007934': {'us-gaap:CommonStockMember': 'NBN'},
                   '0001047469-15-007607': {'us-gaap:CommonStockMember': 'NBN'},
                   '0001104659-13-072680': {'nbn:VotingCommonStockMember': 'NBN'},
                   '0001437749-16-038610': {'nbn:VotingCommonStockMember': 'NBN'},
                   '0001437749-17-015900': {'nbn:VotingCommonStockMember': 'NBN'},
                   '0001437749-18-016964': {'nbn:VotingCommonStockMember': 'NBN'},                   
                   '0001193125-13-087221': {'us-gaap:CommonStockMember': 'PWR'},
                   '0001558370-18-001400': {'linta:QvcGroupCommonClassBMember': 'QRTEB',
                                            'linta:QvcGroupCommonClassMember': 'QRTEA'},
                   '0001437749-14-003872': {'us-gaap:CommonStockMember': 'RUTH'},
                   '0001437749-15-004629': {'us-gaap:CommonStockMember': 'RUTH'},
                   '0001437749-16-026852': {'us-gaap:CommonStockMember': 'RUTH'},
                   '0001437749-17-004007': {'us-gaap:CommonStockMember': 'RUTH'},
                   '0001564590-18-005966': {'us-gaap:CommonStockMember': 'RUTH'},
                   '0001564590-19-005274': {'us-gaap:CommonStockMember': 'RUTH'},
                   '0001558370-15-000135': {'us-gaap:CommonClassAMember': 'STAG'},
                   '0000024545-15-000004': {'us-gaap:CommonClassBMember': 'TAP',
                                            'us-gaap:CommonClassAMember': 'TAP.A'},
                   '0000024545-16-000054': {'us-gaap:CommonClassBMember': 'TAP',
                                            'us-gaap:CommonClassAMember': 'TAP.A'},
                   '0000024545-17-000005': {'us-gaap:CommonClassBMember': 'TAP',
                                            'us-gaap:CommonClassAMember': 'TAP.A'},
                   '0000024545-18-000009': {'us-gaap:CommonClassBMember': 'TAP',
                                            'us-gaap:CommonClassAMember': 'TAP.A'},
                   '0000024545-19-000007': {'us-gaap:CommonClassBMember': 'TAP',
                                            'us-gaap:CommonClassAMember': 'TAP.A'}, 
                   '0001188112-14-000516': {'ucbi:VotingCommonStockMember': 'UCBI'},
                   '0001571049-15-001493': {'ucbi:VotingCommonStockMember': 'UCBI'},
                   '0001571049-16-012197': {'ucbi:VotingCommonStockMember': 'UCBI'}
                   }
    
    logs.configure('file', level=logs.logging.INFO)
    logger = logs.get_logger()    

    shares_reader = MySQLShares()
    reports_reader = MySQLReports()

    ciks = shares_reader.fetch_nasdaq_ciks()
    pb = ProgressBar()
    pb.start(len(ciks))

    data = []
    for cik in ciks:        
        tickers = process_tickers(shares_reader.fetch_tickers(cik))
        for adsh in reports_reader.fetch_adshs(cik):
            sec_shares = process_sec_shares(
                            shares_reader.fetch_sec_shares(adsh))

            if not join_sec_stocks_tickers(sec_shares, tickers):
                logger.set_state(state={'state': str(adsh)})
                logger.info(str(tickers))
                logger.info(str(sec_shares))            
                logger.revoke_state()
            else:
                for shares_list in sec_shares.values():
                    for share in shares_list:
                        if share.ticker != '':
                            data.append((adsh, share.member, share.ticker))
            
        pb.measure()
        print('\r' + pb.message(), end='')
    print()

    pd.DataFrame(data, columns=['adsh', 'member', 'ticker']).to_csv('outputs/member_ticker.csv', index=False)
    shares_reader.close()
    reports_reader.close()

    #*****************************

    # shares_reader = MySQLShares()
    # reports_reader = MySQLReports()
    # ciks = [1029800, 947484, 1067983, 7789, 5272, 1652044, 70858]
    # for cik in ciks:    
    #     tickers = shares_reader.fetch_tickers(cik) 
    #     shares = process_tickers(tickers)
    #     sec_shares = get_sec_shares(cik, shares_reader, reports_reader)
        
    #     if check_shares(shares):
    #         print(cik)
    #         print(tickers)
    #         print(sec_shares)
    #         print(shares)


    # shares_reader.close()
    # reports_reader.close()

    # 'Invesco BLDRS Asia 50 ADR Index Fund'
    # 'RiverNorth Opportunities Fund, Inc.'
    # 'RiverNorth Opportunistic Municipal Income Fund, Inc.'
    # 'Merrill Lynch & Co., Inc.'
    # import json
    # with open('outputs/merrill.json', 'w') as f:
    #     json.dump(find_company_names(['Merrill', 'Lynch'], []), f)