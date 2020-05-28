import datetime as dt

from typing import List, Dict, Any, Iterable

from mysqlio.readers import MySQLReader


class LogReader(MySQLReader):
    def fetch_errors(self,
                     day: dt.date,
                     log_table: str,
                     levelname: str,
                     msg: str) -> List[Dict[str, Any]]:
        assert(log_table in ('logs_parse', 'xbrl_logs'))

        query = f"""
        select id, created, state, module, msg, extra
        from {log_table}
        where created >= %(end)s
            and created <= %(start)s
        """
        params: Dict[str, Any] = {'start': day + dt.timedelta(days=1),
                                  'end': day}
        if levelname != '':
            params['levelname'] = levelname
            query += "and levelname = %(levelname)s"
        if msg != '':
            params['msg'] = msg
            query += "and msg like concat('%', %(msg)s, '%')"

        data = self.fetch(query, params)

        return data


class MailerInfoReader(MySQLReader):
    def fetch_mailer_list(self) -> List[Dict[str, Any]]:
        query = "select * from mailer_list"
        return self.fetch(query=query, params={})

    def fetch_stocks_info(self,
                          day: dt.date,
                          tickers: Iterable[str]) -> List[Dict[str, Any]]:
        query = """select * from stocks_daily
        where trade_date = %s
            and ticker in (__in__)
        """

        return self.fetch_in(query, [day], tickers)

    def fetch_dividents_info(self,
                             day: dt.date,
                             tickers: Iterable[str]) -> List[Dict[str, Any]]:

        query = """select * from stocks_dividents \
            where payment_date >= %s \
                and ifnull(declaration_date, now()) >= %s \
                and ticker in (__in__)
        """

        return self.fetch_in(query, [day, day], tickers)

    def fetch_shares_info(self,
                          day: dt.date,
                          ticker: str) -> List[Dict[str, Any]]:
        query = """select * from stocks_shares \
            where ticker = %s \
                and trade_date <= %s \
            order by trade_date desc limit 2"""

        return self.fetch(query, [ticker, day])

    def fetch_reports_info(self,
                           day: dt.date,
                           ciks: Iterable[int]) -> List[Dict[str, Any]]:
        query = """select * from sec_xbrl_forms \
                    where filed = %s \
                        and cik in (__in__)"""

        return self.fetch_in(query, [day], ciks)
