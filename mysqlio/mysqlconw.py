import mysql.connector  # type :ignore
import mysql.connector.cursor  # type: ignore
import atexit

from mysql.connector import InternalError, Error, ProgrammingError
from typing import Optional, cast

from settings import Settings


class RptCursor(mysql.connector.cursor.CursorBase):
    pass


class RptConnection(object):
    def __init__(self, host: str = Settings.host(),
                 port: int = 3306):
        self.con: Optional[mysql.connector.MySQLConnection] = open_connection_native(
            host=host, port=port)
        atexit.register(self.close)

    def cursor(self, dictionary: bool = False) -> RptCursor:
        if self.con is None:
            raise mysql.connector.ProgrammingError()

        return cast(RptCursor, self.con.cursor(dictionary=dictionary))

    def close(self):
        if self.con is not None:
            try:
                self.con.close()
            except Exception:
                pass
            self.con = None

    def commit(self):
        if self.con is None:
            raise mysql.connector.ProgrammingError()

        self.con.commit()

    def rollback(self):
        if self.con is None:
            raise mysql.connector.ProgrammingError()

        self.con.rollback()

    @property
    def database(self) -> str:
        if self.con is None:
            raise mysql.connector.ProgrammingError()
        return cast(str, self.con.database)


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
        port: int = 3306) -> mysql.connector.MySQLConnection:
    hosts = {"server": "192.168.88.113",
             "remote": "95.31.1.243",
             "localhost": "localhost",
             "mgserver": "192.168.188.149"}
    if host == 'remote':
        port = 3456
    con = mysql.connector.connect(
        user="app",
        password="Burkina!7faso",
        host=hosts[host],
        database=__DATABASE,
        port=port,
        ssl_ca=Settings.ssl_dir() +
        "ca.pem",
        ssl_cert=Settings.ssl_dir() +
        "client-cert.pem",
        ssl_key=Settings.ssl_dir() +
        "client-key.pem",
        connection_timeout=100,
        use_pure=True)

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
