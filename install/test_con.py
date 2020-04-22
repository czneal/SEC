
def test_mysql_connector():
    print('test mysqlconnector...', end='')

    try:

        from mysql.connector import connect
        users = ['app', 'root']

        for user in users:
            config = {"host": "localhost",
                      'port': 3306,
                      'user': user,
                      'password': 'Burkina!7faso',
                      'database': 'reports',
                      'ssl_key': 'e:/mysql/data/client-key.pem',
                      'ssl_cert': 'e:/mysql/data/client-cert.pem',
                      'ssl_ca': 'e:/mysql/data/ca.pem',
                      'use_pure': True
                      }
            try:
                print(f'connect to user: {user} ...', end='')
                con = connect(**config)
                con.close()
                print('ok')
            except Exception as e:
                print('fail')
                print(e)

    except Exception as e:
        print('fail')
        print(e)


def test_mysql_client():
    print('test mysqlclient...', end='')

    try:
        import MySQLdb._mysql as do  # type: ignore
        users = ['app', 'root']

        for user in users:
            config = {'host': '127.0.0.1',
                      'port': 3306,
                      'user': user,
                      'passwd': 'Burkina!7faso',
                      'db': 'reports',
                      'ssl': {'key': 'e:/mysql/data/client-key.pem',
                                'cert': 'e:/mysql/data/client-cert.pem',
                                'ca': 'e:/mysql/data/ca.pem'
                              }
                      }

            try:
                print(f'connect to user: {user} ...', end='')
                con = do.connect(**config)
                con.close()
                print('ok')
            except Exception as e:
                print('fail')
                print(e)

    except Exception as e:
        print('fail')
        print(e)


if __name__ == '__main__':
    test_mysql_connector()
    test_mysql_client()
