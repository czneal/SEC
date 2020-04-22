import unittest
from mysqlio.basicio import activate_test_mode, deactivate_test_mode


class DBTestBase(unittest.TestCase):
    @classmethod
    def setUpClass(DBTestBase):
        activate_test_mode()

    @classmethod
    def tearDownClass(DBTestBase):
        deactivate_test_mode()
