import contextlib
import tensorflow as tf  # type: ignore
import multiprocessing as mp
import multiprocessing.managers
import multiprocessing.synchronize

from multiprocessing.connection import Connection
from queue import Empty
from typing import Dict, List, Tuple, Optional

from indi.modclass import ModelClassifier, load_classifiers

WAIT_FOR_JOB = 0.01
WAIT_FOR_PROC = 5

SERVER_PORT: int = 50000
SERVER_AUTHKEY: bytes = b'PAzqWXo3sy55WMjT'

TagTuple = Tuple[str, str, int]
TagTupleRes = Tuple[str, str, int, int]


class ClassifierCache(object):
    def __init__(self):
        self.cache: Dict[TagTuple, int] = {}

    def predict(self, msg: TagTuple) -> Optional[int]:
        if msg in self.cache:
            return self.cache[msg]

        return None

    def append(self, msg: TagTuple, result: int) -> None:
        self.cache[msg] = result


class Worker(mp.Process):
    def __init__(self,
                 job_queue: mp.Queue,
                 res_queue: mp.Queue,
                 stop_event: mp.synchronize.Event):

        self.job_queue: mp.Queue = job_queue
        self.res_queue: mp.Queue = res_queue
        self.stop_event: mp.synchronize.Event = stop_event
        self.classifies: List[ModelClassifier] = []

        super(Worker, self).__init__()

    def _load(self) -> None:
        self.classifiers = load_classifiers()
        print(f'loaded: {len(self.classifiers)} classifiers')

    def _do_job(self, tag_tuple: TagTuple) -> TagTupleRes:
        parent, child, cl_id = tag_tuple
        if cl_id < len(self.classifiers):
            return(parent,
                   child,
                   cl_id,
                   self.classifiers[cl_id].predict([(parent, child)])[0])
        else:
            return(parent, child, cl_id, -1)

    def run(self):
        try:
            config_tensorflow()
            self._load()
        except Exception as e:
            print(f'worker {self.pid}: unable to start')
            print(f'worker {self.pid}: {str(e)}')
            return

        print(f'worker {self.pid}: started')
        while not self.stop_event.is_set():
            try:
                tag_tuple, job_id = self.job_queue.get(
                    timeout=WAIT_FOR_JOB)
                res = self._do_job(tag_tuple)
                self.res_queue.put((res, job_id))
            except Empty:
                continue
            except Exception as e:
                print(f'worker: {self.pid}: {str(e)}')
            except:
                print('exception unknown')

        print(f'worker {self.pid}: stopped')


class Dispatcher(mp.Process):
    def __init__(self, n_proc: int,
                 pipe_list: Dict[int, Connection],
                 stop_event: mp.synchronize.Event):
        super(Dispatcher, self).__init__()

        self.stop_event = stop_event
        self.n_proc = n_proc
        self.pipes: Dict[int, Connection] = pipe_list
        self.job_queue: mp.Queue = mp.Queue()
        self.res_queue: mp.Queue = mp.Queue()

        self.proccesses: List[Tuple[mp.Process,
                                    mp.synchronize.Event]] = []

        self.cache = ClassifierCache()

    def _start_workers(self) -> None:
        for i in range(self.n_proc):
            e = mp.Event()
            w = Worker(self.job_queue, self.res_queue, e)
            w.start()
            self.proccesses.append((w, e))

    def _stop_workres(self) -> None:
        for w, e in self.proccesses:
            with contextlib.suppress(Exception):
                e.set()
            with contextlib.suppress(Exception):
                w.join(WAIT_FOR_PROC)
            with contextlib.suppress(Exception):
                w.terminate()

        self.proccesses.clear()

    def _pipes_loop(self) -> None:
        """
        inspect pipes, put new jobs into jobs_queue
        """
        for pipe_id in list(self.pipes.keys()):
            pipe = self.pipes[pipe_id]
            try:
                if not pipe.poll(timeout=WAIT_FOR_JOB):
                    continue
                try:
                    msg = pipe.recv()
                    result = self.cache.predict(msg)
                    if result:
                        pipe.send(msg + (result,))
                        continue
                except EOFError:
                    continue

                self.job_queue.put((msg, pipe_id))

            except BrokenPipeError as e:
                pipe.close()
                self.pipes.pop(pipe_id)

    def _result_queue_loop(self) -> None:
        """
        inspect res_queue, put answers into pipes
        """

        while True:
            try:
                msg, pipe_id = self.res_queue.get(timeout=WAIT_FOR_JOB)
            except Empty:
                break

            with contextlib.suppress(Exception):                
                self.cache.append(msg[:3], msg[3])
                self.pipes[pipe_id].send(msg)

    def run(self):
        try:
            self._start_workers()
        except Exception as e:
            print(f'dispatcher: unable to start')
            print(f'dispatcher: {str(e)}')
            return

        print('dispatcher: started')
        while not self.stop_event.is_set():
            try:
                self._pipes_loop()
                self._result_queue_loop()
            except Exception as e:
                print('dispatcher: ', e)
                print('dispatcher: ', type(e))

        self._stop_workres()
        print('dispatcher: stopped')


def config_tensorflow():
    gpus = tf.config.experimental.list_physical_devices('GPU')
    if gpus:
        # Restrict TensorFlow to only allocate 1GB of memory on the first GPU
        try:
            tf.config.experimental.set_virtual_device_configuration(
                gpus[0],
                [tf.config.experimental.VirtualDeviceConfiguration(memory_limit=1024)])
            logical_gpus = tf.config.experimental.list_logical_devices('GPU')
            print(
                len(gpus),
                "Physical GPUs,",
                len(logical_gpus),
                "Logical GPUs")
        except RuntimeError as e:
            # Virtual devices must be set before GPUs have been initialized
            print(e)


class PipeManager(mp.managers.BaseManager):
    def __init__(self, address):
        mp.managers.BaseManager.register(
            'get_pipe', callable=get_pipe)
        mp.managers.BaseManager.register(
            'stop_dispatcher', callable=stop_dispatcher)
        mp.managers.BaseManager.register(
            'stop_server', callable=stop_server)

        super().__init__(address=address, authkey=SERVER_AUTHKEY)

    def get_pipe(self) -> mp.connection.Connection:
        return super().get_pipe()  # type: ignore

    def stop_dispatcher(self):
        return super().stop_dispatcher()

    def stop_server(self):
        return super().stop_server()


def get_pipe() -> Connection:
    p = mp.Pipe()
    if pipe_list:
        i = max(pipe_list)
    else:
        i = 0
    pipe_list[i + 1] = p[0]
    return p[1]


def stop_dispatcher() -> None:
    try:
        stop_event.set()
        dispatcher.join()
    except Exception as e:
        print(f'stop_dispatcher: {str(e)}')


def stop_server() -> None:
    try:
        stop_dispatcher()
        obj_manager.shutdown()
        print('stop object manager')
        # must stop BaseManager but do not know how
    except Exception as e:
        print(f'stop_server: {str(e)}')


if __name__ == '__main__':
    stop_event = mp.Event()
    obj_manager = mp.Manager()
    pipe_list: Dict[int, Connection] = obj_manager.dict()

    pipe_manager = PipeManager(address=('', SERVER_PORT))

    dispatcher = Dispatcher(4, pipe_list, stop_event)
    dispatcher.start()

    server = pipe_manager.get_server()  # type: ignore
    server.serve_forever()
