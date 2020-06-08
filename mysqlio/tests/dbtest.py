import unittest

from typing import List, Dict, Any

from mysqlio.basicio import activate_test_mode, deactivate_test_mode, OpenConnection


class DBTestBase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        activate_test_mode()

    @classmethod
    def tearDownClass(cls):
        deactivate_test_mode()

    def run_set_up_queries(
            self,
            queries: List[str],
            params: List[Dict[str, Any]]):

        with OpenConnection() as con:
            cur = con.cursor()
            for query, param in zip(queries, params):
                cur.execute(query, param)

            con.commit()

    def run_mysql_file(self, filename: str):
        with open(filename) as f:
            queries = [q for q in f.readlines()
                       if (q.lower().startswith('insert') or
                           q.lower().startswith('delete'))]

        params: List[Dict[str, Any]] = [{} for q in queries]
        self.run_set_up_queries(queries, params)
