import multiprocessing as mp
from multiprocessing.managers import BaseManager
import random
import socket


class QueueManager(BaseManager):
    pass


class Worker(mp.Process):
    def __init__(self, queue):
        super(Worker, self).__init__()
        self.queue = queue

    def run(self):
        QueueManager.register('get_pipe')
        m = QueueManager(
            address=('localhost', 50000),
            authkey=b'abracadabra')  # type: ignore
        m.connect()

        pipe = m.get_pipe()  # type: ignore
        what = [('Liabilities', 'LiabilitiesCurrent', 1),
                ('LiabilitiesAndStockholdersEquity', 'Assets', 1)]
        client = self.pid
        for i in range(0, 1000):
            pipe.send(what[0])
            msg = pipe.recv()
            self.queue.put(msg)

        pipe.close()


def main():
    queue = mp.Queue()
    processes = []
    for i in range(0, 5):
        p = Worker(queue)
        p.start()
        processes.append(p)

    while True:
        try:
            msg = queue.get(timeout=1.5)
            print(msg)
        except Exception:
            break

    for p in processes:
        p.join()
        print(f'join process {p.pid}')

    # QueueManager.register('stop_dispatcher')
    # m = QueueManager(
    #     address=('localhost', 50000),
    #     authkey=b'abracadabra')  # type: ignore
    # m.connect()
    # m.stop_dispatcher()


if __name__ == '__main__':
    # QueueManager.register('get_pipe')
    # m = QueueManager(
    #     address=('localhost', 50000),
    #     authkey=b'abracadabra')  # type: ignore
    # m.connect()

    # pipe = m.get_pipe()  # type: ignore
    # what = [('Liabilities', 'LiabilitiesCurrent', 1),
    #         ('LiabilitiesAndStockholdersEquity', 'Assets', 1)]

    # for i in range(0, 10):
    #     pipe.send(what[1])
    #     msg = pipe.recv()
    #     print(msg)

    # pipe.close()

    main()
