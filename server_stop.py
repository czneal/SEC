from server import PipeManager, SERVER_PORT


def main():
    try:
        m = PipeManager(('192.168.88.113', SERVER_PORT))
        # BaseManager.register('stop_server')
        # m = BaseManager(
        #     address=('localhost', 50000),
        #     authkey=b'abracadabra')  # type: ignore

        m.connect()
        m.stop_server()
    except Exception as e:
        print(e)


if __name__ == "__main__":
    main()
