import zipfile
import os
from typing import List, cast
from bs4 import BeautifulSoup

import mpc
import logs
from abstractions import Worker, WriterProxy
from utils import add_root_dir, ProgressBar
from urltools import fetch_with_delay
import mysqlio.readers as r


class Downloader(Worker):
    def __init__(self):
        self.r = FormReader()

    def feed(self, cik: int) -> int:
        links = self.r.fetch_form_links(cik)
        if not links:
            return 0

        add_forms_to_zipfile(f'{cik}.zip', form_links=links)
        return len(links)

    def flush(self):
        pass


def configure_worker() -> Downloader:
    return Downloader()


def configure_writer() -> WriterProxy:
    return WriterProxy()


class FormReader(r.MySQLReader):
    def fetch_form_links(self, cik: int) -> List[str]:
        query = """
        select doc_link from sec_forms
        where cik = %(cik)s
            and form in ('4', '4/A')
        """
        data = self.fetch(query, {'cik': cik})
        return [cast(str, r['doc_link']) for r in data]

    def fetch_nasdaq_ciks(self) -> List[int]:
        query = """
        select cik from nasdaq group by cik
        """
        data = self.fetch(query, params={})
        return [cast(int, row['cik']) for row in data]


def find_form_link(rpt_url: str) -> str:
    rpt_body = fetch_with_delay(rpt_url)
    if rpt_body is None:
        raise ValueError(f'wrong report url: {rpt_url}')

    soup = BeautifulSoup(rpt_body, 'lxml')

    files: List[str] = []
    for node in soup.find_all('a'):
        link = node.get('href')

        lower = link.lower()
        if node.text.lower().endswith('.xml'):
            files.append(link)

    if len(files) == 1:
        return ('https://www.sec.gov' + files[0])

    raise ValueError(
        f'xml report: {rpt_url}, wrong xml form count: {len(files)}')


def add_forms_to_zipfile(zipfile_name: str, form_links: List[str]):
    logger = logs.get_logger(name=add_forms_to_zipfile.__name__)

    forms_dir = add_root_dir('3-4-5')
    if not os.path.exists(forms_dir):
        os.mkdir(forms_dir)

    zipfile_name = os.path.join(forms_dir, zipfile_name)
    try:
        if os.path.exists(zipfile_name):
            zf = zipfile.ZipFile(
                zipfile_name,
                mode='a',
                compression=zipfile.ZIP_DEFLATED,
                compresslevel=5)
        else:
            zf = zipfile.ZipFile(
                zipfile_name,
                mode='w',
                compression=zipfile.ZIP_DEFLATED,
                compresslevel=5)
    except Exception:
        logger.error(
            f'unable write to zip archive {zipfile_name}',
            exc_info=True)
        return

    filelist = set([info.filename for info in zf.filelist])

    pb = ProgressBar()
    pb.start(len(form_links))

    for link in form_links:
        filename = link.split('/')[-1][:20] + '.xml'
        if filename in filelist:
            logger.info(f'{filename} already in zip archive')
            pb.measure()
            continue

        try:
            xml_link = find_form_link(link)
        except ValueError as e:
            logger.error(str(e))
            pb.measure()
            continue

        body = fetch_with_delay(xml_link)
        if body is None:
            logger.error(f'unable download form file: {link}', exc_info=True)
            pb.measure()
            continue
        try:
            zf.writestr(zinfo_or_arcname=filename, data=body)
        except Exception:
            logger.error(
                f'unable write file: {link} to zip file',
                exc_info=True)

        pb.measure()
        # print('\r' + pb.message(), end='')

    print(f'downloaded {len(form_links)}')


def compress():
    import shutil

    pb = ProgressBar()

    forms_dir = add_root_dir('3-4-5')
    for root, _, filenames in os.walk(forms_dir):
        pb.start(len(filenames))
        for filename in filenames:
            try:
                with zipfile.ZipFile(os.path.join(root, filename)) as in_zip, \
                    zipfile.ZipFile(os.path.join(root, 'tmp.zip'),
                                    mode='w',
                                    compression=zipfile.ZIP_DEFLATED,
                                    compresslevel=5) as out_zip:
                    for info in in_zip.filelist:
                        out_zip.writestr(
                            zinfo_or_arcname=info.filename,
                            data=in_zip.open(
                                info.filename).read())
                shutil.copy(src=os.path.join(root, 'tmp.zip'),
                            dst=os.path.join(root, filename))
                pb.measure()
                print('\r' + pb.message(), end='')
            except Exception:
                print(f'{filename}')
        print()


def download(ciks: List[int]) -> int:
    reader = FormReader()
    logs.configure('file', level=logs.logging.INFO)

    for cik in ciks:
        links = reader.fetch_form_links(cik)
        if not links:
            continue

        add_forms_to_zipfile(f'{cik}.zip', links)

    return len(links)


def download_mpc(ciks: List[int]):
    manager = mpc.MpcManager('file', level=mpc.logs.logging.INFO)
    manager.start(to_do=ciks,
                  configure_worker=configure_worker,
                  configure_writer=configure_writer,
                  n_procs=16)


if __name__ == '__main__':
    # reader = FormReader()
    # ciks = reader.fetch_nasdaq_ciks()
    # ciks = [8670]

    # download_mpc(ciks)
    # download(ciks)

    compress()
