import unittest
import multiprocessing as M
import time

from functools import partial
from typing import Any

from mysqlio.tests.dbtest import DBTestBase
from utils import make_absolute
from mysqlio.basicio import MySQLTable, activate_test_mode
from mysqlio.readers import MySQLReader
from mysqlio.writers import MySQLWriter

absfilename = partial(make_absolute, file_loc=__file__)


class SimpleWriter(MySQLWriter):
    def write(self, obj: Any):
        pass


def writer_1():
    activate_test_mode()

    w = SimpleWriter()
    table = MySQLTable('test_table', w.con)
    print('writer_1: connect')
    for i in range(1, 50):
        table.update_row({'id': i, 'info': f'info{i+1000}'}, ['info'], w.cur)
        # table.write_row({'id': i, 'info': f'info{i+1000}'}, w.cur)
        # w.flush()
        w.cur.execute('select * from test_table where id = %s', [i])
        data = w.cur.fetchall()
        time.sleep(0.5)

        w.flush()

        print(f'writer_1: work {i}')

    print('writer_1: end fetch')
    w.close()
    print('writer_1: close')


def writer_2():
    activate_test_mode()

    w = SimpleWriter()
    table = MySQLTable('test_table', w.con)
    print('writer_2: connect')

    time.sleep(3)
    print('writer_2: end sleep')
    table.truncate(w.cur)
    print('writer_2: truncate')

    for i in range(1, 50):
        w.write_to_table(table, [{'id': i, 'info': f'info{i}'}])

    w.cur.execute('select * from test_table where id = 1')
    data = w.cur.fetchall()

    print('writer_2: end write')
    w.flush()
    w.close()
    print('writer_2: close')


@unittest.skip('not implemented')
class _TestReader(DBTestBase):
    def create_test_table(self):
        queries = ["DROP TABLE IF EXISTS `test_table`;",
                   """CREATE TABLE `test_table` (
                    `id` int NOT NULL,
                    `info` varchar(45) NOT NULL,
                    PRIMARY KEY (`id`)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8;"""]

        self.run_set_up_queries(queries, [{}, {}])

    def test_update_while_changed(self):
        self.create_test_table()

        # w1 = SimpleWriter()
        # w2 = SimpleWriter()

        # table = MySQLTable('test_table', w1.con)

        # w1.write_to_table(table, [{'id': 1, 'info': 'info1'}])
        # # w1.flush()

        # table.truncate(w2.cur)
        # w2.write_to_table(table, [{'id': 1, 'info': 'info2'}])
        # w2.write_to_table(table, [{'id': 3, 'info': 'info3'}])
        # w2.flush()

        # w1.write_to_table(table, [{'id': 3, 'info': 'info4'}])
        # table.update_row(
        #     {'id': 3, 'info': 'info5'},
        #     update_fields=['info'],
        #     cur=w1.cur)
        # w1.flush()

        wr1 = M.Process(
            target=writer_1,
            name='writer_1')

        wr2 = M.Process(
            target=writer_2,
            name='writer_2')

        wr1.start()
        wr2.start()

        wr1.join()
        wr2.join()


if __name__ == '__main__':
    unittest.main()
