import re
import json
import os
import nltk  # type: ignore

from collections import defaultdict
from typing import List, Dict, Union, Any
from zipfile import ZipFile, ZIP_BZIP2

from bs4 import BeautifulSoup
from bs4.element import Comment  # type: ignore

import logs

from mysqlio.basicio import MySQLTable
from mysqlio.writers import MySQLWriter
from utils import ProgressBar


class HtmlTextWriter(MySQLWriter):
    def __init__(self):
        super().__init__()

        self.html_reports = MySQLTable('html_reports', con=self.con)

    def write(self, obj: Any):
        """obj must by tuple (adsh: str, text: str)"""

        self.write_to_table(
            self.html_reports, {
                'adsh': obj[0], 'report_text': obj[1]})


def text_from_zip(filename: str) -> str:
    with ZipFile(filename) as f:
        return f.open('10-K.txt').read().decode('utf8')


def html_from_zip(filename: str) -> str:
    with ZipFile(filename) as f:
        return f.open('10-K.html').read().decode('utf8')


def clean_text_to_zip(filename: str, text: str):
    if os.path.exists(filename):
        f = ZipFile(filename, mode='a')
    else:
        f = ZipFile(filename, mode='w', compression=ZIP_BZIP2)

    try:
        f.writestr('10-K.clean.txt', text)
    finally:
        f.close()


def extract_words(text: str) -> List[str]:
    if not text:
        return []

    special = ['']

    text = text.lower()
    text = text.replace('\u201a', ' ').replace('\u201c', ' ').replace(
        '\u2014', '-').replace('\u2019', "'").replace('\u00ae', ' ')
    text = re.sub(r'[\s]+', ' ', text)
    text = re.sub(r'[\.\,\?\!=;:\-]+\s+', ' ', text)
    text = re.sub(r'[<>\(\)\{\}/]+', ' ', text)

    return text.split(' ')


def main(html_dir: str):
    logger = logs.get_logger()

    writer = HtmlTextWriter()

    for root, dirs, filenames in os.walk(html_dir):
        pb = ProgressBar()
        pb.start(len(filenames))

        logger.info(f'start reading texts {len(filenames)}')

        for filename in filenames:
            try:
                logger.debug(f'open {filename}')
                txt = text_from_zip(os.path.join(root, filename))

                if txt.startswith('%PDF-1.5'):
                    logger.info(f'{filename} is pdf')
                    continue

                logger.debug(f'tokenize {filename}')
                tokens = nltk.word_tokenize(txt)

                text = ' '.join(tokens)

                # logger.debug(f'write clean text to {filename}')
                # clean_text_to_zip(filename, text)

                logger.debug(f'write clean text of {filename} to mysql')
                writer.write((filename[:20], text))
                writer.flush()

                pb.measure()
                print('\r' + pb.message(), end='')
            except Exception:
                logger.error('unexpected error', exc_info=True)

        print()
        logger.info(f'finish reading texts {len(filenames)}')

    writer.close()


if __name__ == '__main__':
    logs.configure('file', level=logs.logging.DEBUG)
    main('d:/sec/2016/02/html')
    main('d:/sec/2016/03/html')
