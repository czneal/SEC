import MySQLdb  # type: ignore
import MySQLdb.cursors  # type: ignore
import MySQLdb.connections  # type: ignore
import os
import atexit

from typing import cast, Optional
from MySQLdb import InternalError, Error, ProgrammingError

from settings import Settings


class RptCursor(MySQLdb.cursors.BaseCursor):
    pass


class RptConnection(object):
    def __init__(self, host: str = Settings.host(),
                 port: int = 3306):
        self.con: Optional[MySQLdb.connections.Connection] = open_connection_native(
            host=host, port=port)
        atexit.register(self.close)
        self.__database: str = ''

    def cursor(self, dictionary: bool = False) -> RptCursor:
        if self.con is None:
            raise MySQLdb.ProgrammingError()

        if dictionary:
            return cast(RptCursor, self.con.cursor(MySQLdb.cursors.DictCursor))
        else:
            return cast(RptCursor, self.con.cursor())

    def close(self):
        if self.con is not None:
            try:
                self.con.close()
            except Exception:
                pass
            self.con = None

    def commit(self):
        if self.con is None:
            raise MySQLdb.ProgrammingError()

        self.con.commit()

    def rollback(self):
        if self.con is None:
            raise MySQLdb.ProgrammingError()

        self.con.rollback()

    @property
    def database(self) -> str:
        if self.con is None:
            raise MySQLdb.ProgrammingError()
        if self.__database != '':
            return self.__database

        cur = self.con.cursor()
        cur.execute('select database()')
        d = cur.fetchall()
        self.__database = cast(str, d[0][0])

        return self.__database


__DATABASE = 'reports'


def activate_test_mode():
    globals()['__DATABASE'] = 'test_'


def deactivate_test_mode():
    globals()['__DATABASE'] = 'reports'


def open_connection(host: str = Settings.host(),
                    port: int = 3306) -> RptConnection:
    return RptConnection(host=host, port=port)


def open_connection_native(
        host: str = Settings.host(),
        port: int = 3306) -> MySQLdb.connections.Connection:
    hosts = {"server": "192.168.88.113",
             "remote": "95.31.1.243",
             "localhost": "localhost",
             "mgserver": "192.168.188.149"}
    if host == 'remote':
        port = 3456

    config = {
        'host': hosts[host],
        'port': port,
        'user': 'app',
        'passwd': 'Burkina!7faso',
        'db': __DATABASE,
        'ssl': {
            'key': os.path.join(
                Settings.ssl_dir(),
                'client-key.pem'),
            'cert': os.path.join(
                Settings.ssl_dir(),
                'client-cert.pem'),
            'ca': os.path.join(
                Settings.ssl_dir(),
                'ca.pem')}}

    con = MySQLdb.connect(**config)
    return con


if __name__ == "__main__":
    try:
        con = RptConnection()
        cur = con.cursor(dictionary=True)
        cur.execute('select * from companies limit 10')
        data = cur.fetchall()
        print(data)
        con.close()

    except Exception as e:
        if con:
            con.close()
