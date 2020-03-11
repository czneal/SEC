from server import PipeManager, SERVER_PORT


def main():
    try:
        ip = {'mgserver': '195.222.11.86',
              'server': '192.168.88.113'}

        s = input('enter server to stop: ')
        m = PipeManager((ip[s], SERVER_PORT))
        # BaseManager.register('stop_server')
        # m = BaseManager(
        #     address=('localhost', 50000),
        #     authkey=b'abracadabra')  # type: ignore

        print(f'try to connect to server: {ip[s]}...', end='')
        m.connect()
        print('ok')
        print(f'try to stop server: {ip[s]}...', end='')
        m.stop_server()
        print('ok')
    except Exception as e:
        print('fail')
        print(e)


if __name__ == "__main__":
    main()
