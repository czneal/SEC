from multiprocessing.managers import BaseManager


class QueueManager(BaseManager):
    pass


if __name__ == '__main__':
    QueueManager.register('get_queue')
    m = QueueManager(address=('localhost', 50000), authkey=b'abracadabra')
    m.connect()
    queue = m.get_queue()
    queue.put('hello')
    queue.put(None)
