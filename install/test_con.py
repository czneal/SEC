import os
from settings import Settings


def test_mysql_connector():
    print('test mysqlconnector...')

    try:

        from mysql.connector import connect
        users = ['app', 'root', 'admin']

        for user in users:
            config = {
                "host": Settings.host(),
                'port': Settings.port(),
                'user': user,
                'password': Settings.password(),
                'database': 'reports',
                'ssl_key': os.path.join(
                    Settings.ssl_dir(),
                    'client-key.pem'),
                'ssl_cert': os.path.join(
                    Settings.ssl_dir(),
                    'client-cert.pem'),
                'ssl_ca': os.path.join(
                    Settings.ssl_dir(),
                    'ca.pem'),
                'use_pure': True}
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
    print('test mysqlclient...')

    try:
        import MySQLdb._mysql as do  # type: ignore
        users = ['app', 'root']

        for user in users:
            config = {'host': Settings.host(),
                      'port': Settings.port(),
                      'user': user,
                      'passwd': Settings.password(),
                      'db': 'reports',
                      'ssl': {'key': os.path.join(
                          Settings.ssl_dir(),
                          'client-key.pem'),
                'cert': os.path.join(
                          Settings.ssl_dir(),
                          'client-cert.pem'),
                'ca': os.path.join(
                          Settings.ssl_dir(),
                          'ca.pem')
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
