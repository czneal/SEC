import os
from itertools import product

import nltk  # type: ignore

from zipfile import ZipFile
from typing import Tuple, List, Iterable

import logs
import mpc

from fts.writers import HtmlTextWriter
from abstractions import Worker
from utils import ProgressBar, year_month_dir


class HtmlTextWorker(Worker):
    def __init__(self):
        self.tok = Tokenizer()
        self.logger = logs.get_logger('HtmlTextWorker')

    def feed(self, job: str) -> Tuple[str, str]:
        filename = str(job)

        if not os.path.exists(filename):
            self.logger.info(f'{filename} doesnt exist')

        txt = text_from_zip(filename=filename)
        if txt.startswith('%PDF-1.5'):
            self.logger.info(f'{filename} is pdf')
            return ('', '')
            
        txt = self.tok.clean_text(txt)
        adsh = filename[-24:-4]

        return (adsh, txt)


    def flush(self):
        pass


class Tokenizer():
    def __init__(self):
        self.tokenize = nltk.word_tokenize

    def clean_text(self, text: str) -> str:
        tokens = self.get_tokens(text)
        return ' '.join(tokens)        

    def approve_token(self, token: str) -> bool:
        if len(token) > 1:
            return True
        if len(token) <= 1 and token in (u',.?!"\'#$%@&*:;^-()[]<>\{\}\\/'):
            return True

        return False

    def get_tokens(self, text: str) -> List[str]:
        tokens = nltk.word_tokenize(text)
        return [t for t in filter(self.approve_token, tokens)]



def text_from_zip(filename: str) -> str:
    with ZipFile(filename) as f:
        return f.open('10-K.txt').read().decode('utf8')


def html_from_zip(filename: str) -> str:
    with ZipFile(filename) as f:
        return f.open('10-K.html').read().decode('utf8')

def html_to_mysql_single(
        filenames: List[str],
        log_handler: str,
        log_level: int):
    
    logs.configure(
        handler_name=log_handler,
        level=log_level)
    
    logger = logs.get_logger(name='html_to_mysql')
    worker = HtmlTextWorker()
    writer = HtmlTextWriter()

    pb = ProgressBar()
    pb.start(len(filenames))

    logger.info(f'start reading texts: {len(filenames)}')
    for filename in filenames:
        try:
            logger.debug(f'read {filename}')
            obj = worker.feed(filename)

            logger.debug(f'write to mysql {filename}')
            writer.write(obj)
            writer.flush()            
        except Exception:
            logger.error('unexpected error', exc_info=True)
        
        pb.measure()
        print('\r' + pb.message(), end='')

    print()
    logger.info(f'finish reading texts {len(filenames)}')

    writer.close()

def html_to_mysql_mpc(
        filenames: List[str],
        log_handler: str,
        log_level: int,
        n_procs: int):
    
    assert n_procs>1, 'mpc version more effective on several processes'
    
    man = mpc.MpcManager(handler_name=log_handler, level=log_level)

    logger = logs.get_logger(name='html_to_mysql')
    logger.info(f'start reading texts: {len(filenames)}')
    
    man.start(to_do=filenames,
        configure_writer=HtmlTextWriter,
        configure_worker=HtmlTextWorker,
        n_procs=n_procs)

    logger.info(f'finish reading texts {len(filenames)}')    


def gen_filenames(
        years: Iterable[int], 
        months: Iterable[int]) -> List[str]:
    
    filenames: List[str] = []

    for y, m in product(years, months):
        dir_name = os.path.join(year_month_dir(y, m), 'html')
        for root, dirs, files in os.walk(dir_name):
            filenames.extend([os.path.join(root, f) for f in files])
    
    return filenames

if __name__ == '__main__':
    filenames = gen_filenames(years=[2016], months=[1,2,3])

    html_to_mysql_mpc(filenames=filenames[:12],
        log_handler='file',
        log_level=logs.logging.DEBUG,
        n_procs=4)