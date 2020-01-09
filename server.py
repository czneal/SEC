from multiprocessing import Process, Queue
from multiprocessing.managers import BaseManager


class Worker(Process):
    def __init__(self, q):
        self.q = q
        super(Worker, self).__init__()

    def run(self):
        while True:
            msg = self.q.get()
            if msg is None:
                print('stopped')
                break
            print(str(msg))


if __name__ == '__main__':
    queue = Queue()  # type: ignore
    w = Worker(queue)
    w.start()

    BaseManager.register('get_queue', callable=lambda: queue)
    m = BaseManager(address=('', 50000), authkey=b'abracadabra')
    s = m.get_server()
    s.serve_forever()
