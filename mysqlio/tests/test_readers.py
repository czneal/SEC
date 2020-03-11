import unittest

from mysqlio.readers import MySQLReader


class TestReader(unittest.TestCase):
    def test_fetch(self):
        r = MySQLReader()

        data = r._fetch('select * from mgnums limit 10;')
        self.assertEqual(len(data), 10)

        data = r._fetch(
            'select * from mgnums where adsh = %(adsh)s limit 10;',
            {'adsh': '0000002178-13-000014'})
        self.assertEqual(len(data), 10)


if __name__ == '__main__':
    unittest.main()
