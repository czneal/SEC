import unittest

from mysqlio.readers import MySQLReader
from mysqlio.tests.dbtest import DBTestBase  # type: ignore


class TestReader(DBTestBase):
    def test_fetch(self):
        r = MySQLReader()

        data = r.fetch('select * from mgnums limit 10;')
        self.assertEqual(len(data), 10)

        data = r.fetch(
            'select * from mgnums where adsh = %(adsh)s limit 10;',
            {'adsh': '0000002178-13-000014'})
        self.assertEqual(len(data), 10)


if __name__ == '__main__':
    unittest.main()
