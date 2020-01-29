import multiprocessing as mp
from multiprocessing.managers import BaseManager
import random
from itertools import product


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
        what = [('Liabilities', 'LiabilitiesCurrent'),
                ('LiabilitiesAndStockholdersEquity', 'Assets'),
                ('LiabilitiesAndStockholdersEquity', 'Assets'),
                ('Liabilities', 'LiabilitiesCurrent'),
                ('Liabilities', 'LiabilitiesNoncurrent')]

        for r, c in product(range(10), range(8)):
            for msg in what:
                pipe.send((msg[0], msg[1], c))
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
    # what = [('Liabilities', 'LiabilitiesCurrent', 0),
    #         ('LiabilitiesAndStockholdersEquity', 'Assets', 0),
    #         ('LiabilitiesAndStockholdersEquity', 'Assets', 2),
    #         ('Liabilities', 'LiabilitiesCurrent', 1),
    #         ('Liabilities', 'LiabilitiesNoncurrent', 1)]

    # for r in range(100):
    #     for msg in what:
    #         pipe.send(msg)
    #         msg = pipe.recv()
    #         print(msg)

    # pipe.close()

    main()

    # from server import ClassifierCache
    # cache = ClassifierCache()
    # for i, w in enumerate(what):
    #     res = cache.predict(w)
    #     if res:
    #         print(w, res)
    #     else:
    #         cache.append(w, i)
