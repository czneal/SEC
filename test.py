import datetime as dt

from xbrlxml.xbrlrss import XBRLEnumerator, MySQLEnumerator
from mysqlio.xbrlfileio import records_to_mysql
from xbrldown.download import download_rss

if __name__ == '__main__':

    rss = MySQLEnumerator()
    rss.set_filter_method('all', dt.date(2019, 1, 1))
    records = rss.filing_records()
    for record, file_link in records:
        print(record.adsh, file_link)

    print(len(records))
