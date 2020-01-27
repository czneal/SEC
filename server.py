import contextlib
import multiprocessing as mp
import multiprocessing.managers
import multiprocessing.synchronize

from multiprocessing.connection import Connection
from queue import Empty
from typing import Dict, List, Tuple, Optional

from test import SingleParentAndChild, prepare_models
WAIT_FOR_JOB = 0.01

TagTuple = Tuple[str, str, int]

class ClassifierCache(object):
    def __init__(self):
        self.cache: Dict[TagTuple, int] = {}

    def predict(self, msg: TagTuple) -> Optional[int]:
        if msg in self.cache:
            return self.cache[msg] + 10
        
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

        super(Worker, self).__init__()

    def _load(self) -> None:
        model = prepare_models()
        self.cl = SingleParentAndChild('dictionary.csv', model, 40)

    def run(self):
        self._load()
        print(f'started {self.pid}')
        while not self.stop_event.is_set():
            try:
                (parent, child, cl_id), msg_id = self.job_queue.get(
                    timeout=WAIT_FOR_JOB)
            except Empty:
                continue
            y = self.cl.predict([(parent, child)])
            self.res_queue.put(
                ((parent, child, cl_id), y[0], msg_id))


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
            e.set()
            w.join()
        self.proccesses.clear()

    def run(self):
        self._start_workers()

        print('dispatcher started')
        while not self.stop_event.is_set():
            try:
                # print(f'\rpipes: {len(self.pipes)}', end='')
                for pipe_id in list(self.pipes.keys()):
                    pipe = self.pipes[pipe_id]
                    try:
                        if not pipe.poll(timeout=WAIT_FOR_JOB):
                            # print(f'\rpipe {i} nothing to read', end='')
                            continue
                        try:
                            msg = pipe.recv()
                            result = self.cache.predict(msg)
                            if result:
                                pipe.send((msg, result))
                                continue                            
                        except EOFError:
                            continue

                        self.job_queue.put((msg, pipe_id))

                    except BrokenPipeError as e:
                        pipe.close()
                        self.pipes.pop(pipe_id)

                while True:
                    try:
                        msg, result, pipe_id = self.res_queue.get(timeout=0.01)
                        if pipe_id in self.pipes:
                            with contextlib.suppress(Exception):
                                self.cache.append(msg, result)
                                self.pipes[pipe_id].send((msg, result))
                    except Empty:
                        break
            except Exception as e:
                print(e)
                print(type(e))

        self._stop_workres()
        print('dispatcher stopped')


def get_pipe() -> Connection:
    p = mp.Pipe()
    if pipe_list:
        i = max(pipe_list)
    else:
        i = 0
    pipe_list[i + 1] = p[0]
    return p[1]


def stop_dispatcher() -> None:
    stop_event.set()


if __name__ == '__main__':
    stop_event = mp.Event()
    obj_manager = mp.Manager()
    pipe_list: Dict[int, Connection] = obj_manager.dict()

    mp.managers.BaseManager.register(
        'get_pipe', callable=get_pipe)
    mp.managers.BaseManager.register(
        'stop_dispatcher', callable=stop_dispatcher)
    bm = mp.managers.BaseManager(
        address=('', 50000),
        authkey=b'abracadabra')

    dispatcher = Dispatcher(4, pipe_list, stop_event)
    dispatcher.start()

    server = bm.get_server()  # type: ignore
    server.serve_forever()
