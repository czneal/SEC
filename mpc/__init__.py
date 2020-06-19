# -*- coding: utf-8 -*-
"""
Created on Tue Oct 15 10:54:08 2019

@author: Asus
"""
import os
import time

import multiprocessing as mp
import multiprocessing.synchronize
import multiprocessing.managers

import logs
from settings import Settings
from mpc.util import MAX_WAIT_TIME, WAIT_FOR_JOB
from mpc.util import StopObject, terminate_procs

from queue import Empty
from typing import Any, List, Dict, Type, cast, Callable, Optional
from utils import ProgressBar
from abstractions import Writer, Worker, JobType, WriteType


def configure_logging(queue: mp.Queue, level: int) -> None:
    logs.configure(handler_name='queue',
                   level=level,
                   use_state=True,
                   queue=queue)


def writer_proc(
        write_queue: mp.Queue,
        log_queue: mp.Queue,
        info_queue: mp.Queue,
        stop_event: mp.synchronize.Event,
        configure_writer: Callable[[], Writer],
        level: int,
        total: int) -> None:

    configure_logging(log_queue, level)
    logger = logs.get_logger(name=__name__)
    logger.set_state(state={'state': 'mpc.writer'}, extra={})

    try:
        logger.debug(msg='configure writer')
        writer_obj = configure_writer()

        pb = ProgressBar()
        pb.start(total)

        info_queue.put('success')
    except Exception:
        logger.error(msg='fail to start writer', exc_info=True)
        logger.revoke_state()
        info_queue.put('fail')
        return

    logger.debug(msg='main loop')
    while not stop_event.is_set():
        try:
            value = write_queue.get(timeout=WAIT_FOR_JOB)
            if isinstance(value, StopObject):
                break
        except Empty:
            continue

        try:
            writer_obj.write(obj=value)
        except Exception:
            logger.error(msg='write error', exc_info=True)

        pb.measure()
        print('\r' + pb.message(), end='')
    print()

    logger.debug(msg='flush writer')
    try:
        writer_obj.flush()
    except Exception:
        logger.error('flush writer failed', exc_info=True)

    logger.revoke_state()
    info_queue.put_nowait('done')


def worker_proc(
        jobs_queue: mp.Queue,
        write_queue: mp.Queue,
        log_queue: mp.Queue,
        info_queue: mp.Queue,
        stop_event: mp.synchronize.Event,
        configure_worker: Callable[[], Worker],
        level: int) -> None:

    configure_logging(log_queue, level)
    logger = logs.get_logger(name=__name__)
    logger.set_state(
        state={
            'state': 'mpc.worker.' + mp.current_process().name},
        extra={})

    try:
        logger.debug('configure worker')
        worker_obj = configure_worker()
        info_queue.put('success')
    except Exception:
        logger.error(msg='failed to start worker', exc_info=True)
        logger.revoke_state()
        info_queue.put('fail')
        return

    logger.debug('worker main loop')
    while not stop_event.is_set():
        try:
            job = jobs_queue.get(timeout=WAIT_FOR_JOB)
            if isinstance(job, StopObject):
                break
        except Empty:
            continue

        try:
            value = worker_obj.feed(job)
            write_queue.put_nowait(value)
        except Exception:
            logger.error(msg='worker error', exc_info=True)
        finally:
            try:
                jobs_queue.task_done()
            except Exception:
                pass

    logger.debug('flush worker')
    try:
        worker_obj.flush()
    except Exception:
        logger.error(msg='flush worker failed', exc_info=True)

    logger.revoke_state()
    info_queue.put_nowait('done')


class MpcManager():
    def __init__(self, handler_name: str, level: int):
        assert handler_name in {'file', 'mysql'}

        # self.man = mp.managers.SyncManager()
        # self.man.start()
        # self.write_queue = self.man.Queue()  # type: ignore
        # self.jobs_queue = self.man.Queue()  # type: ignore

        # self.writer_stop_event = self.man.Event()
        # self.worker_stop_event = self.man.Event()

        self.write_queue = mp.JoinableQueue()  # type: ignore
        self.jobs_queue = mp.JoinableQueue()  # type: ignore

        self.writer_stop_event = mp.Event()
        self.worker_stop_event = mp.Event()

        logs.LOGMAN.start_mpc_logging(handler_name, level)
        self.log_queue = logs.LOGMAN.queue
        configure_logging(self.log_queue, level)

        self.level = level

    def start(self,
              to_do: List[JobType],
              configure_writer: Callable[[], Writer],
              configure_worker: Callable[[], Worker],
              n_procs: int = 4) -> None:
        assert n_procs > 0

        try:
            logger = logs.get_logger(name=__name__)
            logger.set_state(state={'state': 'mpc.manager.start'}, extra={})

            info_queue = mp.Queue()  # type: ignore
            # create writer and worker processes
            writer = mp.Process(
                target=writer_proc,
                name='writer',
                kwargs={
                    'write_queue': self.write_queue,
                    'log_queue': self.log_queue,
                    'info_queue': info_queue,
                    'stop_event': self.writer_stop_event,
                    'level': self.level,
                    'total': len(to_do),
                    'configure_writer': configure_writer})
            processes = [
                mp.Process(
                    target=worker_proc,
                    kwargs={'jobs_queue': self.jobs_queue,
                            'write_queue': self.write_queue,
                            'stop_event': self.worker_stop_event,
                            'info_queue': info_queue,
                            'log_queue': self.log_queue, 'level': self.level,
                            'configure_worker': configure_worker})
                for i in range(n_procs)]

            logger.debug('start writer process')
            writer.start()
            if info_queue.get() == 'fail':
                raise Exception()

            logger.debug('start worker processes')
            for p in processes:
                p.start()
                if info_queue.get() == 'fail':
                    raise Exception()

            logger.debug('feed jobs to jobs_queue')
            for job in to_do:
                self.jobs_queue.put(job)

        except Exception:
            pass
        finally:
            logger.debug('join worker processes')
            terminate_procs(processes, self.jobs_queue, self.worker_stop_event)

            logger.debug('join writer process')
            terminate_procs([writer], self.write_queue, self.writer_stop_event)

            logger.revoke_state()


class TestWorker(Worker):
    def __init__(self):
        self.logger = logs.get_logger(name=__name__)

    def feed(self, job: JobType) -> str:
        # raise Exception('test feed exception')
        time.sleep(2.0)
        self.logger.info(
            multiprocessing.current_process().name +
            ' - ' +
            str(job))
        return multiprocessing.current_process().name + ' - ' + str(job)

    def flush(self) -> None:
        # raise Exception('test worker flush exception')
        self.logger.info(multiprocessing.current_process().name + ' - flush')


class TestWriter(Writer):
    def __init__(self):
        self.logger = logs.get_logger(name=__name__)

    def write(self, obj: WriteType) -> None:
        # raise Exception('test write exception')
        time.sleep(2.0)
        self.logger.info(
            multiprocessing.current_process().name +
            ' - ' +
            str(obj))

    def flush(self) -> None:
        # raise Exception('test write flush exception')
        self.logger.info(multiprocessing.current_process().name + ' - flush')


def test_configure_writer() -> Writer:
    # raise Exception('test_configure_writer')
    return TestWriter()


def test_configure_worker() -> Worker:
    # raise Exception('test_configure_worker')
    return TestWorker()


def main():
    manager = MpcManager('file', logs.logging.DEBUG)
    jobs = ['job-' + str(i) for i in range(20)]
    manager.start(jobs, test_configure_writer, test_configure_worker)


if __name__ == '__main__':
    pass
