# -*- coding: utf-8 -*-
"""
Created on Tue Oct 15 10:57:40 2019

@author: Asus
"""

import atexit
import logging
import logging.handlers
import json
import multiprocessing as multi
import multiprocessing.synchronize
import sys
import copy
import traceback

from queue import Empty
from typing import Dict, Any, cast, Callable, Union, Optional, Tuple, List

import utils
import mpc.util as mpcu
import mysqlio.basicio as do
from algos.xbrljson import ForDBJsonEncoder
from settings import Settings

"""
logging customization

Single process mode:
    - Call configure function. It accepts
        handler_name: {'mysql', 'file'},
        use_state: {True, False},
        queue: None
    - Call get_logger() than log

Multiprocess mode:
    from main process call logs.LOGMAN.start_mpc_logging() to start listener process
    from worker process call configure function. It accepts:
        handler_name: {'queue'},
        use_state: {True, False},
        queue: should be set to logs.LOGMAN.queue
"""


def format_record(record, handler: logging.Handler) -> Any:
    assert handler.formatter is not None

    data = record.__dict__.copy()
    exc_text = None
    if data['exc_info'] is not None:
        exc_text = handler.formatter.formatException(
            record.__dict__['exc_info'])
    if data['exc_text'] is not None:
        exc_text = data['exc_text']
    if exc_text is not None:
        if 'extra' not in data:
            data['extra'] = {}
        data['extra']['exc_info'] = exc_text.split('\n')

    if 'extra' in data:
        data['extra'] = json.dumps(data['extra'],
                                   indent=3,
                                   cls=ForDBJsonEncoder)
    else:
        data['extra'] = ''
    data['created'] = handler.formatter.formatTime(record)
    if 'state' not in data:
        data['state'] = ''

    return data


class StateLogger(logging.Logger):
    state: Dict[str, str] = {}
    extra: Dict[str, str] = {}
    _states: List[Dict[str, str]] = []
    _extras: List[Dict[str, str]] = []

    def set_state(self, state: Dict[str, str], extra: Dict[str, str]={}) -> None:
        StateLogger._states.append(StateLogger.state.copy())
        StateLogger._extras.append(StateLogger.extra.copy())

        StateLogger.state = state.copy()
        StateLogger.extra = extra.copy()

    def revoke_state(self):
        try:
            StateLogger.state = StateLogger._states.pop()
            StateLogger.extra = StateLogger._extras.pop()
        except IndexError:
            StateLogger.state = {}
            StateLogger.extra = {}

    def makeRecord(self, name, level, fn, lno, msg, args, exc_info,
                   func=None, extra=None, sinfo=None):
        if StateLogger.extra:
            if extra is not None:
                extra = {**extra, **StateLogger.extra}
            else:
                extra = StateLogger.extra.copy()
        if extra is not None:
            extra = {'extra': extra}

        rv = super().makeRecord(name, level, fn, lno, msg, args, exc_info,
                                func, extra, sinfo)

        if StateLogger.state.get('state', None) is None:
            StateLogger.state['state'] = ''

        for k, v in StateLogger.state.items():
            rv.__dict__[k] = v

        return rv


class MySQLHandler(logging.Handler):
    def __init__(self, level: int = logging.NOTSET):
        super().__init__(level)
        self.con = do.open_connection()
        self.table = do.MySQLTable(
            'logs_parse', self.con, use_simple_insert=True)

    def emit(self, record):
        try:
            data = format_record(record, self)
            cur = self.con.cursor(dictionary=True)
            self.table.write_row(data, cur)
            self.con.commit()

        except Exception as e:
            raise e

    def close(self):
        try:
            self.con.commit()
            self.con.close()
            del self.con
        except Exception:
            pass


class LocalHandler(logging.FileHandler):
    def emit(self, record):
        try:
            data = format_record(record, self)
            for line in data['extra'].replace('\r', '').split('\n'):
                self.stream.write('{0}\t{1}\t{2}\t{3}\t{4}\t{5}\n'.format(
                    data['created'],
                    data['state'],
                    data['name'],
                    data['levelname'],
                    data['msg'],
                    line))
            self.flush()
        except Exception as e:
            raise e


class MyQueueHandler(logging.Handler):
    """Custom QueueHandler dispatch records into logging queue

    be shure that record object is picklebal
    handlers.QueueHandler
    """

    def __init__(self, queue: multi.Queue):
        super().__init__()
        self.queue = queue

    def emit(self, record):
        record = copy.copy(record)
        record.args = None
        if record.exc_info is not None:
            record.exc_text = self.formatter.formatException(
                record.exc_info)
            record.exc_info = None

        self.queue.put_nowait(record)


def configure_handler(handler_name: str,
                      queue: Optional[multi.Queue] = None) -> logging.Handler:
    assert handler_name in {'mysql', 'file', 'queue', 'silent'}

    handler: logging.Handler
    if handler_name == 'mysql':
        handler = MySQLHandler()
    elif handler_name == 'file':
        handler = LocalHandler(
            filename=utils.add_app_dir(
                Settings.log_filename()))
    elif handler_name == 'queue':
        assert queue is not None
        handler = MyQueueHandler(queue)
    elif handler_name == 'silent':
        handler = logging.NullHandler()

    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    formatter.default_msec_format = '%s.%06d'
    handler.setFormatter(formatter)

    return handler

CONFIGURED = False

def configure(
        handler_name: str,
        level: int = logging.DEBUG,
        use_state: bool = True,
        queue: Optional[multi.Queue] = None) -> None:
    """setup logging configuration

    handler_name accepts
        'mysql' - logs into mysql database
        'file' - logs into local file with global.settings.log_filename
        'queue' - put log records into queue, queue argument should be set
        'silent' - logs nothing
    use_state
        True - use StateLogger for log record processing,
        before using logging should call StateLogger.set_state()
        False - use logging.Logger
    queue
        multiprocessing.Queue object to queue log records
    """

    if use_state:
        logging.setLoggerClass(klass=StateLogger)
    root = logging.getLogger()
    handler = configure_handler(handler_name, queue)
    handler.setLevel(level)
    root.addHandler(handler)
    root.setLevel(level)

    globals()['CONFIGURED'] = True


def get_logger(name: str = 'root') -> StateLogger:
    if not CONFIGURED:
        configure('silent')

    return cast(StateLogger, logging.getLogger(name))


# Multiprocess configuration
def listener_proc(queue: multi.Queue,
                  stop_event: multi.synchronize.Event,
                  info_queue: multi.Queue,
                  configurer: Callable[..., None],
                  conf_kwargs: Dict[str, Union[str, int]]) -> None:

    try:
        configurer(**conf_kwargs)
        info_queue.put('success')

        while not stop_event.is_set():
            # wait for log record
            try:
                record = queue.get(block=True, timeout=mpcu.WAIT_FOR_JOB)
                if isinstance(record, mpcu.StopObject):
                    break
            except Empty:
                continue

            # process log record
            try:
                logger = logging.getLogger(record.name)
                logger.handle(record)
            except Exception:
                print('problem while logging')
                traceback.print_exc(file=sys.stderr)
    except Exception:
        print('problem while start logging')
        traceback.print_exc(file=sys.stderr)
        info_queue.put('fail')
    finally:
        logging.shutdown()


def start_mpc_logging(handler_name: str,
                      level: int = logging.DEBUG) -> Tuple[Optional[multi.Process],
                                                           Optional[multi.Queue],
                                                           Optional[multi.synchronize.Event]]:

    # queue = multi.Queue()  # type: ignore
    queue = multi.Queue()  # type: ignore
    stop_event = multi.Event()
    info = multi.Queue()  # type: ignore

    listener = multi.Process(target=listener_proc,
                             name='logging.listener',
                             args=(queue,
                                   stop_event,
                                   info,
                                   configure, {'handler_name': handler_name,
                                               'level': level,
                                               'use_state': False,
                                               'queue': queue}))

    print('start logging listener process...', end='')
    listener.start()
    if info.get() == 'fail':
        print('fail')
        return (None, None, None)

    print('ok')
    return (listener, queue, stop_event)


# def stop_mpc_logging(listener: Optional[multi.Process],
#                      queue: Optional[multi.Queue],
#                      stop_event: Optional[multi.synchronize.Event]) -> None:
#     if listener is None and queue is None and stop_event is None:
#         return

#     try:
#         if queue is not None:
#             queue.join()
#             print('close log queue...', end='')
#             queue.close()
#             print('ok')
#     except Exception:
#         print('fail')
#         traceback.print_exc(file=sys.stderr)

#     try:
#         print('set stop event for log listener...', end='')
#         if stop_event is not None:
#             stop_event.set()
#         print('ok')
#     except Exception:
#         print('fail')
#         traceback.print_exc(file=sys.stderr)

#     try:
#         if listener is not None:
#             print('stop log listener process...', end='')
#             listener.join(timeout=1.0)
#             if listener.exitcode is None:
#                 listener.terminate()
#             print('ok')
#     except Exception:
#         print('fail')
#         traceback.print_exc(file=sys.stderr)


class LogManager():
    def __init__(self):
        self.listener: Optional[multi.Process] = None
        self.queue = multi.Queue()
        self.stop_event = multi.Event()

    def start_mpc_logging(
            self,
            handler_name: str,
            level: int = logging.DEBUG) -> None:
        assert handler_name in {'mysql', 'file'}

        (self.listener,
         self.queue,
         self.stop_event) = start_mpc_logging(handler_name, level)

        if self.listener is None:
            raise Exception("couldn't start multiprocess logging")

        atexit.register(LOGMAN.stop_mpc_logging)

    def stop_mpc_logging(self) -> None:
        print('join log listener process...', end='')
        if self.listener is None:
            print('ok')
            return

        # stop_mpc_logging(self.listener, self.queue, self.stop_event)
        mpcu.terminate_procs([self.listener], self.queue,
                             self.stop_event, silent=True)
        print('ok')


LOGMAN = LogManager()

if __name__ == '__main__':
    pass
