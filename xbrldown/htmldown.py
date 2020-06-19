import re
import zipfile
import os
import datetime as dt

from typing import Union, Any, List, Tuple
from bs4 import BeautifulSoup, Comment  # type: ignore

import mpc
import logs

from urltools import fetch_with_delay
from abstractions import Worker, WriterProxy
from mysqlio.readers import MySQLReader
from xbrldown.download import create_download_dirs, year_month_dir


def get_html_report_link(submis_link: str) -> str:
    body = fetch_with_delay(submis_link)
    bs = BeautifulSoup(body, 'lxml')

    html_report_link = ""
    pdf_report_link = ""

    for r in bs.findAll('tr'):
        if r.find('td', text=re.compile(r'10-K(/A)?')):
            href = r.find('a')
            link = href.attrs['href']
            if link.lower().endswith('pdf'):
                pdf_report_link = link
            else:
                html_report_link = link

    if html_report_link == "":
        if pdf_report_link:
            return "https://sec.gov" + pdf_report_link
        return ""

    if html_report_link.lower().startswith('/ix?doc'):
        html_report_link = html_report_link[8:]

    return "https://sec.gov" + html_report_link


def tag_visible(element):
    if element.parent.name in [
        'style',
        'script',
        'head',
        'title',
        'meta',
            '[document]']:
        return False

    if isinstance(element, Comment):
        return False
    return True


def html_to_text(html: Union[str, bytes]) -> str:
    bs = BeautifulSoup(html, 'lxml')
    texts = bs.findAll(text=True)
    text = '\n'.join([t for t in filter(tag_visible, texts) if str(t) != ''])

    return text


def archive_html_file(
        filename: str,
        report_link: str,
        overwrite: bool = False):
    if os.path.exists(filename) and not overwrite:
        return

    content = fetch_with_delay(report_link)
    if content is None:
        return

    zfile = zipfile.ZipFile(filename, mode='w', compression=zipfile.ZIP_BZIP2)
    zfile.writestr('10-K.html', content)
    zfile.writestr('10-K.txt', html_to_text(content))

    zfile.close()


class HtmlWorker(Worker):
    def __init__(self, overwrite: bool = False):
        self.overwrite = overwrite

    def feed(self, job: Any) -> bool:
        """job must by tuple: (adsh, filed: datetime.date, submis_link)"""
        try:
            adsh = str(job[0])
            assert(isinstance(job[1], dt.date))
            filed = job[1]
            submis_link = str(job[2])
        except Exception:
            raise AttributeError(
                'job must by tuple: (adsh, filed: datetime.date, submis_link)')

        report_link = get_html_report_link(submis_link)
        if report_link == '':
            return False

        zip_filename = os.path.join(
            year_month_dir(filed.year, filed.month),
            'html',
            adsh + '.zip')

        create_download_dirs(filed.year, filed.month)

        archive_html_file(
            filename=zip_filename,
            report_link=report_link,
            overwrite=self.overwrite)

        return True

    def flush(self):
        pass


def download_mpc(
        jobs: List[Tuple[str, dt.date, str]],
        n_procs: int,
        log_handler: str,
        log_level: int):

    man = mpc.MpcManager(log_handler, level=log_level)
    logger = logs.get_logger('download html mpc')
    logger.info(f'start to download {len(jobs)} html reports')

    man.start(to_do=jobs,
              configure_worker=HtmlWorker,
              configure_writer=WriterProxy,
              n_procs=n_procs)

    logger.info(f'finish to download {len(jobs)} html reports')


def main():
    r = MySQLReader()
    data = r.fetch(
        "select * from sec_forms where form like '10-k' and filed>='2016-01-01'")
    r.close()

    jobs = [(row['adsh'], row['filed'], row['doc_link']) for row in data]

    download_mpc(jobs, 10, log_handler='file', log_level=logs.logging.INFO)


if __name__ == '__main__':
    main()
