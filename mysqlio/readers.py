import atexit
import logs
import datetime as dt

from typing import Dict, List, Any, Tuple, cast, Iterable, Union

import mysqlio.basicio as do
from algos.scheme import Chapters
from algos.xbrljson import loads


class MySQLReader():
    def __init__(self):
        self.con = do.open_connection()

        self.cur = self.con.cursor(dictionary=True)
        atexit.register(self.close)

    def fetch(self, query: str,
              params: Union[
                  Dict[str, Any],
                  List[Any]] = {}) -> List[Dict[str, Any]]:
        try:
            self.con.commit()
            if params:
                self.cur.execute(query, params)
            else:
                self.cur.execute(query)
            return cast(List[Dict[str, Any]], self.cur.fetchall())
        except Exception:
            logs.get_logger(
                name=__name__, default_handler_name='console').error(
                'error while execute sql statement',
                exc_info=True)
            return []

    def fetch_in(
            self,
            query: str,
            params: List[Any],
            params_in: Iterable[Any]) -> List[Dict[str, Any]]:
        """fetches data form MySQL table
        params: list of positional params
        params_in: iterable of params used as MySQL in clause
        query should contains __in__ marker to insert (params_in[0], params_in[1], ...)
        """

        params_in = list(params_in)

        if len(params_in) == 0:
            return self.fetch(query.replace('__in__', ' '), params)

        assert('__in__' in query)

        template = ', '.join('%s' for _ in range(len(params_in)))
        query = query.replace('__in__', template)

        return self.fetch(query, params + params_in)

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

    def fetch_unconnected_ciks(self) -> List[int]:
        data = self.fetch(q_get_unconnected_ciks)
        return [cast(int, r['cik']) for r in data]

    def fetch_possible_ticker(
            self, cik: int, member: str) -> List[Tuple[str, str]]:
        data = self.fetch(q_get_possible_ticker, {'cik': cik,
                                                  'member': member})
        return [(cast(str, r['member']), cast(str, r['ticker']))
                for r in data]


class MySQLReports(MySQLReader):
    def find_latest_report(self, cik: int) -> str:
        try:
            self.cur.execute(q_find_latest_report, (cik,))
            result = self.cur.fetchall()
            if result:
                return cast(str, result[0]['adsh'])
            return ''
        except Exception:
            return ''

    def fetch_adshs(self, cik: int) -> List[str]:
        try:
            self.cur.execute(q_get_adshs, {'cik': cik})
            return [cast(str, r['adsh']) for r in self.cur.fetchall()]
        except Exception:
            return []

    def fetch_chapters(self, adsh: str) -> Chapters:
        """returns Chapters object by given adsh
        """
        query = """select * from reports where adsh = %(adsh)s"""
        data = self.fetch(query, {'adsh': adsh})
        if data:
            return cast(Chapters, loads(data[0]['structure']))

        return {}


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
select cik from nasdaq where cik is not null group by cik;
"""

q_get_unconnected_ciks = """
select cik
from sec_shares ss, reports r
where ticker is null
    and r.adsh = ss.adsh
group by cik
"""

q_get_possible_ticker = """
select member, ticker
from sec_shares ss, reports r
where r.cik = %(cik)s
	and r.adsh = ss.adsh
    and member = %(member)s
    and ticker is not null
group by member, ticker;
"""

if __name__ == '__main__':
    pass
