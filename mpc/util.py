import logging
import multiprocessing as mp
from typing import List, cast

WAIT_FOR_JOB = 0.01
MAX_WAIT_TIME = 5.0


class StopObject():
    pass


class LoggerProxy(logging.Logger):
    def debug(self, *args, **kwargs) -> None:
        pass

    def info(self, *args, **kwargs) -> None:
        pass

    def error(self, *args, **kwargs) -> None:
        pass


def terminate_procs(procs: List[mp.Process],
                    queue: mp.Queue,
                    stop_event: mp.synchronize.Event,
                    silent=False) -> bool:
    try:
        if silent:
            logger = cast(logging.Logger, LoggerProxy(name=__name__))
        else:
            logger = logging.getLogger(name=__name__)

        logger.debug(msg='send stop message to processes')
        for _ in procs:
            queue.put(StopObject())

        logger.debug(msg='join processes')
        for proc in procs:
            # if proc is not running - just close
            if not proc.is_alive():
                proc.close()
                logger.debug(msg=f'process {proc.name} joined')
                continue

            # join process
            try:
                proc.join()
                logger.debug(msg=f'process {proc.name} joined')
            except KeyboardInterrupt:
                logger.debug(msg='keybord interrupt set stop_event')
                stop_event.set()
                break

        # if something still alive
        for proc in procs:
            try:
                if proc.is_alive():
                    proc.join(MAX_WAIT_TIME)
                    logger.debug(msg=f'process {proc.name} joined')
                if proc.is_alive():
                    proc.terminate()
                    logger.debug(msg=f'process {proc.name} terminated')
            except ValueError:
                continue

        # close queue
        logger.debug('close queue')
        queue.close()

        return True
    except Exception:
        logger.error(msg='unhandled exception', exc_info=True)
        return False
