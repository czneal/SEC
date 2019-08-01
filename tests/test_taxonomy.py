# -*- coding: utf-8 -*-

import unittest
import structure.taxonomy

class TestDefaultTaxonomy(unittest.TestCase):
    def test_calcscheme(self):
        deftax = structure.taxonomy.DefaultTaxonomy()
        
        with self.subTest(i='bs'):
            calc = deftax.calcscheme('bs', 'roleuri1', 2019)
            self.assertEqual(calc.roleuri, 'roleuri1')
            self.assertEqual(len(calc.getnodes()), 614)
        
        with self.subTest(i='is'):
            calc = deftax.calcscheme('is', 'roleuri1', 2019)
            self.assertEqual(calc.roleuri, 'roleuri1')
            self.assertEqual(len(calc.getnodes()), 129)
        
        with self.subTest(i='cf'):
            calc = deftax.calcscheme('cf', 'roleuri1', 2019)
            self.assertEqual(calc.roleuri, 'roleuri1')
            self.assertEqual(len(calc.getnodes()), 85)
            
        with self.subTest(i='raise assert'):
            self.assertRaises(AssertionError, deftax.calcscheme,
                              'abc', 'roleuri1', 2019)
        
if __name__ == '__main__':
    unittest.main()
        

