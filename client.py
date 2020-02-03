from itertools import product
import multiprocessing as mp
from indi.remclass import load_remote_classifiers
from settings import Settings
from server import PipeManager, SERVER_PORT


class Worker(mp.Process):
    def __init__(self, queue):
        super(Worker, self).__init__()
        self.queue = queue

    def run(self):
        classifiers = load_remote_classifiers()
        print(f'worker {self.pid}: loaded')

        what = [('Liabilities', 'LiabilitiesCurrent'),
                ('LiabilitiesAndStockholdersEquity', 'Assets'),
                ('LiabilitiesAndStockholdersEquity', 'Assets'),
                ('Liabilities', 'LiabilitiesCurrent'),
                ('Liabilities', 'LiabilitiesNoncurrent')]

        for r, c in product(range(5), range(8)):
            res = classifiers[c].predict(what)
            self.queue.put(list(zip(what, res)))

        print(f'worker {self.pid}: stopped')


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
        p.join(5)
        print(f'join process {p.pid}')


if __name__ == '__main__':
    # from multiprocessing.managers import BaseManager

    # class QueueManager(BaseManager):
    #     pass

    # QueueManager.register('get_pipe')
    # m = QueueManager(
    #     address=(Settings.server_address(), 50000),
    #     authkey=b'PAzqWXo3sy55WMjT')  # type: ignore
    # m = PipeManager(address=(Settings.server_address(), SERVER_PORT))
    # m.connect()

    # pipe = m.get_pipe()  # type: ignore
    # classifiers = load_remote_classifiers()
    # what = [('Liabilities', 'LiabilitiesCurrent'),
    #         ('LiabilitiesAndStockholdersEquity', 'Assets'),
    #         ('LiabilitiesAndStockholdersEquity', 'Assets'),
    #         ('Liabilities', 'LiabilitiesCurrent'),
    #         ('Liabilities', 'LiabilitiesNoncurrent')]

    # for r, c in product(range(10), range(8)):
    #     res = classifiers[c].predict(what)
    #     print(list(zip(what, res)))

    main()
