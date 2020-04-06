
def test_mysql_connector():
    print('test mysqlconnector')
    
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
        print(f'connect to user: {user} ...', end='')    
        try:
            con = connect(**config)
            con.close()
            print('ok')
        except Exception as e:
            print('fail')
            print(e)
        
def test_mysql_client():
    print('test mysqlclient')
    
    import MySQLdb._mysql as do
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

        print(f'connect to user: {user} ...', end='')    
        try:
            con = do.connect(**config)
            con.close()
            print('ok')
        except Exception as e:
            print('fail')
            print(e)
    
if __name__ == '__main__':
    test_mysql_connector()
    test_mysql_client()
