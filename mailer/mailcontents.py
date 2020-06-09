import datetime as dt
import json

from typing import List, Dict, Any, cast, Set, Iterator, Tuple, Union, Optional
from abc import ABCMeta, abstractmethod

from algos.xbrljson import ForDBJsonEncoder
from mailer.readers import LogReader, MailerInfoReader
from xbrlxml.xbrlrss import FileRecord, record_from_str


def process_data(data: List[Dict[str, Any]],
                 json_field: str) -> List[Dict[str, Any]]:

    d: List[Dict[str, Any]] = []
    for row in data:
        if row[json_field] != '':
            new_row = row.copy()
            new_row[json_field] = json.loads(row[json_field])
            d.append(new_row)
    return d


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
    data = process_data(data, json_field='extra')

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


class MailerList():
    def __init__(self):
        self.subscribers: Dict[str,
                               List[Tuple[Subscription, InfoRequest]]] = {}

        self.subscriptions: List[Subscription] = []

        self.formatter = JsonFormatter()

        self.subscriptions.append(
            Subscription(
                data_bank=StocksInfo(),
                formatter=self.formatter)
        )
        self.subscriptions.append(
            Subscription(
                data_bank=DividentsInfo(),
                formatter=self.formatter)
        )
        self.subscriptions.append(
            Subscription(
                data_bank=SharesInfo(),
                formatter=self.formatter)
        )
        self.subscriptions.append(
            Subscription(
                data_bank=ReportsInfo(),
                formatter=self.formatter)
        )
        self.subscriptions.append(
            Subscription(
                data_bank=LogInfo(),
                formatter=self.formatter)
        )

        self.naming = {'stocks': 0,
                       'dividents': 1,
                       'shares': 2,
                       'reports': 3,
                       'logs': 4}
        self.request_class = {0: StocksRequest,
                              1: DivRequest,
                              2: SharesRequest,
                              3: ReportsRequest,
                              4: LogRequest,
                              }

    def read_metadata(self):
        self.subscribers = {}

        r = MailerInfoReader()
        mlist = r.fetch_mailer_list()
        r.close()

        mlist = process_data(mlist, json_field='metadata')

        for row in mlist:
            index = self.naming.get(row['subscription'], -1)
            if index == -1:
                continue

            request = self.request_class[index](row['metadata'])
            self.subscriptions[index].data_bank.append_request(request)

            self.subscribers.setdefault(
                row['email'],
                []).append(
                (self.subscriptions[index], request))

    def read_data(self, day: dt.date):
        for sub in self.subscriptions:
            sub.read_data(day)

    def get_messages(self) -> Iterator[Tuple[str, str]]:
        for subscriber, subscriptions in self.subscribers.items():
            messages: List[str] = []
            for sub, request in subscriptions:
                m = sub.get_message(request)
                if m:
                    messages.append(m)

            msg = "\n".join(messages)

            yield subscriber, msg


class InfoRequest(metaclass=ABCMeta):
    @abstractmethod
    def __init__(self, metadata: Any):
        pass


class StocksRequest(InfoRequest):
    def __init__(self, metadata: Any):
        self.data: Dict[str, Tuple[float, float]] = metadata.copy()


class DivRequest(InfoRequest):
    def __init__(self, metadata: Any):
        self.data: List[str] = metadata.copy()


class SharesRequest(InfoRequest):
    def __init__(self, metadata: Any):
        self.tickers: Set[str] = set([ticker for ticker in metadata])


class ReportsRequest(InfoRequest):
    def __init__(self, metadata: Any):
        self.ciks: Dict[int, List[str]] = {
            int(cik): forms for cik, forms in metadata.items()}


class LogRequest(InfoRequest):
    supported_types = frozenset(['fatal', 'stocks', 'xbrl', 'shares'])

    def __init__(self, metadata: Any):
        assert(set(metadata).issubset(self.supported_types))

        self.types: Set[str] = set(metadata)


class InfoResponse():
    def __init__(self, description: str):
        self.description = description
        self.data: List[Dict[str, Any]] = []


class StocksResponse(InfoResponse):
    def __init__(self):
        self.description = 'Stocks Info'
        self.data: List[Dict[str, Union[str, float]]] = []


class DivResponse(InfoResponse):
    def __init__(self):
        self.description = "Dividends Info"
        self.data: List[Dict[str, Union[str, dt.date, float, None]]] = []


class SubscriptionInfo(metaclass=ABCMeta):
    @abstractmethod
    def reset(self):
        pass

    @abstractmethod
    def append_request(self, request: InfoRequest):
        pass

    @abstractmethod
    def read(self, day: dt.date):
        pass

    @abstractmethod
    def get_info(self, request: InfoRequest) -> InfoResponse:
        pass


class StocksInfo(SubscriptionInfo):
    def __init__(self):
        self.tickers: Set[str] = set()
        self.data: Dict[str, float] = {}

    def reset(self):
        self.tickers = set()
        self.data = {}

    def append_request(self, request: InfoRequest):
        request = cast(StocksRequest, request)
        self.tickers.update([ticker.upper() for ticker in request.data.keys()])

    def read(self, day: dt.date):
        self.data = {}

        r = MailerInfoReader()
        data = r.fetch_stocks_info(day=day, tickers=self.tickers)
        r.close()

        for row in data:
            self.data[row['ticker'].upper()] = float(row['close'])

    def get_info(self, request: InfoRequest) -> InfoResponse:
        request = cast(StocksRequest, request)

        response = StocksResponse()
        for ticker, [low, high] in request.data.items():
            ticker = ticker.upper()
            if ticker not in self.data:
                continue
            close = self.data[ticker]
            if close >= high:
                response.data.append({'ticker': ticker,
                                      'close': close,
                                      'more than': high})
            if close <= low:
                response.data.append({'ticker': ticker,
                                      'close': close,
                                      'less than': low})

        return response


class DividentsInfo(SubscriptionInfo):
    def __init__(self):
        self.tickers: Set[str] = set()
        self.data: Dict[str, Dict[str, dt.date, float, None]] = {}

    def reset(self):
        self.tickers = set()
        self.data = {}

    def append_request(self, request: InfoRequest):
        request = cast(DivRequest, request)
        self.tickers.update([ticker.upper() for ticker in request.data])

    def read(self, day: dt.date):
        self.data = {}

        r = MailerInfoReader()
        data = r.fetch_dividents_info(day=day, tickers=self.tickers)
        r.close()

        for row in data:
            try:
                amount: Optional[float] = float(row['amount'])
            except (ValueError, TypeError):
                amount = None

            self.data[row['ticker'].upper()] = {
                'payment_date': row['payment_date'],
                'record_date': row['record_date'],
                'declaration_date': row['declaration_date'],
                'ex_eff_date': row['ex_eff_date'],
                'type': row['type'],
                'amount': amount}

    def get_info(self, request: InfoRequest) -> DivResponse:
        request = cast(DivRequest, request)

        response = DivResponse()
        for ticker in request.data:
            ticker = ticker.upper()
            if ticker not in self.data:
                continue

            d: Dict[str, Union[str, dt.date, float, None]] = {'ticker': ticker}
            d.update(self.data[ticker])
            response.data.append(d)

        return response


class SharesInfo(SubscriptionInfo):
    def __init__(self):
        self.tickers: Set[str] = set()
        self.data: Dict[str, Tuple[float, float, float]] = {}

    def reset(self):
        self.tickers = set()
        self.data = {}

    def append_request(self, request: InfoRequest):
        request = cast(SharesInfo, request)
        self.tickers.update([ticker.upper() for ticker in request.tickers])

    def read(self, day: dt.date):
        self.data = {}

        r = MailerInfoReader()
        for ticker in self.tickers:
            data = r.fetch_shares_info(day=day, ticker=ticker)

            if len(data) != 2:
                continue

            if data[0]['trade_date'] != day:
                continue

            shares_now = float(data[0]['shares'])
            shares_pre = float(data[1]['shares'])
            diff = shares_now - shares_pre

            self.data[ticker] = (shares_now, shares_pre, diff)

        r.close()

    def get_info(self, request: InfoRequest) -> InfoResponse:
        request = cast(SharesRequest, request)

        response = InfoResponse(description='Shares Changes Info')

        for ticker in request.tickers:
            ticker = ticker.upper()
            if ticker not in self.data:
                continue
            shares_now, shares_pre, diff = self.data[ticker]

            d = {'ticker': ticker,
                 'shares_now': shares_now,
                 'shares_pre': shares_pre,
                 'change': diff, }
            response.data.append(d)

        return response


class ReportsInfo(SubscriptionInfo):
    def __init__(self):
        self.data: Dict[int, List[FileRecord]] = {}
        self.ciks: Set[int] = set()

    def reset(self):
        self.data = {}
        self.ciks = set()

    def append_request(self, request: InfoRequest):
        request = cast(ReportsRequest, request)

        self.ciks.update(request.ciks.keys())

    def read(self, day=dt.date):
        self.data = {}

        r = MailerInfoReader()
        data = r.fetch_reports_info(day, ciks=self.ciks)
        r.close()

        for row in data:
            record = record_from_str(row['record'])
            self.data.setdefault(row['cik'], []).append(record)

    def get_info(self, request: InfoRequest) -> InfoResponse:
        request = cast(ReportsRequest, request)
        response = InfoResponse('Reports Info')

        for cik, forms in request.ciks.items():
            if cik not in self.data:
                continue

            for form in forms:
                for record in self.data[cik]:
                    if (record.form_type.startswith('10-K') and form == 'y'
                        or
                            record.form_type.startswith('10-Q') and form == 'q'):
                        d = record.__dict__.copy()
                        response.data.append(d)

        return response


class LogInfo(SubscriptionInfo):
    def __init__(self):
        self.data: Dict[str, List[Dict[str, Any]]] = {}
        self.types: Set[str] = set()

    def reset(self):
        self.data = {}
        self.types = set()

    def append_request(self, request: InfoRequest):
        request = cast(LogRequest, request)

        self.types.update(request.types)

    def load_extra(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        for row in data:
            if row['extra'] != '':
                row['extra'] = json.loads(row['extra'])

        return data

    def read(self, day=dt.date):
        self.data = {}

        r = LogReader()
        for t in self.types:
            if t == 'fatal':
                self.data.setdefault(t, []).extend(
                    self.load_extra(
                        r.fetch_errors(
                            day=day,
                            log_table='logs_parse',
                            levelname='error',
                            msg='')))
            if t == 'stocks':
                self.data.setdefault(t, []).extend(
                    self.load_extra(
                        r.fetch_errors(
                            day=day,
                            log_table='logs_parse',
                            levelname='warning',
                            msg='denied'
                        )
                    )
                )
                self.data[t].extend(
                    self.load_extra(
                        r.fetch_errors(
                            day=day,
                            log_table='logs_parse',
                            levelname='warning',
                            msg='ticker AAPL shares doesn'
                        )
                    )
                )
            if t == 'xbrl':
                self.data.setdefault(t, []).extend(
                    self.load_extra(
                        r.fetch_errors(
                            day=day,
                            log_table='xbrl_logs',
                            levelname='error',
                            msg=''
                        )
                    )
                )
            if t == 'shares':
                self.data.setdefault(t, []).extend(
                    self.load_extra(
                        r.fetch_errors(
                            day=day,
                            log_table='xbrl_logs',
                            levelname='warning',
                            msg='share-ticker'
                        )
                    )
                )
        r.close()

    def get_info(self, request: InfoRequest) -> InfoResponse:
        request = cast(LogRequest, request)
        response = InfoResponse('Log Info')

        for t in request.types:
            if t not in self.data:
                continue
            for r in self.data[t]:
                d = r.copy()
                d['type'] = t
                response.data.append(d)

        return response


class InfoFormatter(metaclass=ABCMeta):
    @abstractmethod
    def format_message(self, response: InfoResponse) -> str:
        pass


class JsonFormatter(InfoFormatter):
    def format_message(self, response: InfoResponse) -> str:
        if not response.data:
            return ''

        return (response.description +
                '\n' +
                json.dumps(response.data, cls=ForDBJsonEncoder, indent=2))


class Subscription():
    def __init__(self,
                 data_bank: SubscriptionInfo,
                 formatter: InfoFormatter):

        self.data_bank = data_bank
        self.formatter = formatter

    def read_data(self, day: dt.date):
        self.data_bank.read(day)

    def get_message(self, request: InfoRequest) -> str:
        response = self.data_bank.get_info(request)

        return self.formatter.format_message(response)


if __name__ == "__main__":
    mail_list = MailerList()
    mail_list.read_metadata()
    mail_list.read_data(dt.date(2020, 5, 5))

    for s, m in mail_list.get_messages():
        print(f"{s}: {m}")
