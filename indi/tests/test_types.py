import unittest

import indi.types as t


class TetsEmptyPlace(unittest.TestCase):
    def test_eph_operators(self):
        self.assertTrue(t.eph == t.eph)
        self.assertTrue(t.eph == t.EmptyPlace())
        self.assertTrue(t.eph != 1.0)
        self.assertTrue(t.eph != 0.0)

        self.assertFalse(t.eph != t.eph)
        self.assertFalse(t.eph != t.EmptyPlace())
        self.assertFalse(t.eph == 1.0)
        self.assertFalse(t.eph == 0.0)

        self.assertEqual(t.eph + t.eph, t.eph)
        self.assertEqual(t.eph + 1.0, 1.0)
        self.assertEqual(2.0 + t.eph, 2.0)

        self.assertEqual(t.eph - t.eph, t.eph)
        self.assertEqual(t.eph - 1.0, -1.0)
        self.assertEqual(2.0 - t.eph, 2.0)

        self.assertEqual(abs(t.eph), t.eph)
        self.assertEqual(-t.eph, t.eph)

        self.assertFalse(t.eph)

        self.assertEqual(t.eph / 2.0, t.eph)
        self.assertEqual(2.0 / t.eph, t.eph)
        self.assertEqual(t.eph / t.eph, t.eph)

        self.assertEqual(t.eph * 2.0, t.eph)
        self.assertEqual(2.0 * t.eph, t.eph)
        self.assertEqual(t.eph * t.eph, t.eph)

    def test_iter_operations(self):
        with self.subTest(test='nansum'):
            self.assertEqual(t.nansum([1.0, 2.0]), (3.0, 2))
            self.assertEqual(t.nansum([None, 1.0, 2.0]), (3.0, 2))
            self.assertEqual(t.nansum([1.0, 2.0, None]), (3.0, 2))
            self.assertEqual(t.nansum([1.0, t.eph, 2.0]), (3.0, 2))
            self.assertEqual(t.nansum([1.0, t.eph, 2.0, None]), (3.0, 2))
            self.assertEqual(
                t.nansum([t.eph, None, t.EmptyPlace()]),
                (t.eph, 0))

        with self.subTest(test='nanprod'):
            self.assertEqual(t.nanprod([1.0, 2.0]), (2.0, 2))
            self.assertEqual(t.nanprod([None, 1.0, 2.0]), (2.0, 2))
            self.assertEqual(t.nanprod([1.0, 2.0, None]), (2.0, 2))
            self.assertEqual(t.nanprod([1.0, t.eph, 2.0]), (2.0, 2))
            self.assertEqual(t.nanprod([1.0, t.eph, 2.0, None]), (2.0, 2))
            self.assertEqual(
                t.nanprod([t.eph, None, t.EmptyPlace()]),
                (t.eph, 0))

        with self.subTest(test='nanmin'):
            self.assertEqual(t.nanmin([1.0, 2.0]), 1.0)
            self.assertEqual(t.nanmin([t.eph, 2.0]), 2.0)
            self.assertEqual(t.nanmin([None, 2.0]), 2.0)
            self.assertEqual(t.nanmin([-12.0, t.EmptyPlace()]), -12.0)
            self.assertEqual(t.nanmin([None, None]), t.eph)
            self.assertEqual(t.nanmin([t.eph, None]), t.eph)

        with self.subTest(test='nanmax'):
            self.assertEqual(t.nanmax([1.0, 2.0]), 2.0)
            self.assertEqual(t.nanmax([t.eph, 2.0]), 2.0)
            self.assertEqual(t.nanmax([None, 2.0]), 2.0)
            self.assertEqual(t.nanmax([-12.0, t.EmptyPlace()]), -12.0)
            self.assertEqual(t.nanmax([None, None]), t.eph)
            self.assertEqual(t.nanmax([t.eph, None]), t.eph)

        with self.subTest(test='assign'):
            self.assertEqual(t.assign(t.eph, None), t.eph)
            self.assertEqual(t.assign(None, None), None)
            self.assertEqual(t.assign(None, t.eph), None)
            self.assertEqual(t.assign(t.eph, 2.0), 2.0)
            self.assertEqual(t.assign(2.0, t.eph), 2.0)
            self.assertEqual(t.assign(2.0, None), 2.0)
            self.assertEqual(t.assign(2.0, 3.0), 2.0)


if __name__ == '__main__':
    unittest.main()
