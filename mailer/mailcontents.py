import datetime as dt
import json

from abc import ABCMeta, abstractmethod
from typing import List, Dict, Any, cast

from mysqlio.readers import MySQLReader
from algos.xbrljson import ForDBJsonEncoder


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


def process_data(data: List[Dict[str, Any]]) -> None:
    """modify data argument, convert 'extra' field from str to dict"""

    for row in data:
        if row['extra'] != '':
            row['extra'] = json.loads(row['extra'])


def make_parse_error_msg(
        day: dt.date,
        log_table: str,
        levelname: str,
        msg: str) -> str:
    r = LogReader()
    data = r.fetch_errors(
        day=day,
        log_table=log_table,
        levelname=levelname,
        msg=msg)
    process_data(data)

    r.close()

    if data:
        return json.dumps(data, indent=2, cls=ForDBJsonEncoder)
    return ''


def make_parse_info(day: dt.date) -> str:
    parse_err = make_parse_error_msg(
        day=day,
        log_table='logs_parse',
        levelname='error',
        msg='')

    stocks_err = make_parse_error_msg(
        day=day,
        log_table='logs_parse',
        levelname='warning',
        msg='nasdaq site denied request for tikcer')

    xbrl_err = make_parse_error_msg(
        day=day,
        log_table='xbrl_logs',
        levelname='error',
        msg='')

    msg = f"""{day}

    Here fatal errors:
{parse_err}

    Here stocks scrape errors:
{stocks_err}

    Here XBRL files parse errors:
{xbrl_err}"""

    return msg


if __name__ == "__main__":
    inf = make_parse_info(day=dt.date(2020, 5, 21))
    print(inf)
