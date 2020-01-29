import multiprocessing as mp
from multiprocessing.managers import BaseManager


class QueueManager(BaseManager):
    pass


def main():
    try:
        BaseManager.register('stop_server')
        m = BaseManager(
            address=('localhost', 50000),
            authkey=b'abracadabra')  # type: ignore
        m.connect()

        m.stop_server()
    except Exception as e:
        print(e)


if __name__ == "__main__":
    main()
