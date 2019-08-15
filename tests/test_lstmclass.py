# -*- coding: utf-8 -*-

from unittest.mock import patch
import unittest, mock
from parameterized import parameterized

from tests.resource_indi import Data
import indi.lstmclass as CL
import indi.exceptions

class TestFilter(unittest.TestCase):
    """
    test filtering by leaf/all elements
    test filtering by start_chapter
    test filtering by start_tag
    """
    @patch('indi.lstmmodels.Models')
    def setUp(self, mock_models):
        instance = mock_models.return_value

        self.m = mock.Mock()
        instance.models = {'model1': self.m}

        self.cl = CL.SingleParentAndChild(
            fdict='dictionary.csv',
            fmodel='model1',
            start_chapter='bs',
            start_tag='us-gaap:LiabilitiesAndStockholdersEquity',
            max_len=40,
            leaf=False)

    @parameterized.expand([
        (True, Data.predict_single_ones, Data.structure, (29, 7), 22),
        (False, Data.predict_single_ones, Data.structure, (29, 7), 29),
        (True, Data.predict_single, Data.structure, (29, 7), 14),
        (False, Data.predict_single, Data.structure, (29, 7), 20),
        (False, Data.predict_single, {}, (0, 7), 0)])
    def test_predict(self, leaf,
                     predict, structure,
                     answer_shape, answer_sum):
        """
        test predict in different situations
        """

        self.cl.leaf = leaf
        self.m.predict.side_effect = predict

        self.cl.predict_all(structure)
        #Data.predicted.append(self.cl.predicted.copy())

        self.assertEqual(self.cl.predicted.shape, answer_shape)
        self.assertEqual(self.cl.predicted['class'].sum(), answer_sum)

    @parameterized.expand([
        ('bs', 'us-gaap:LiabilitiesAndStockholdersEquity',
         Data.predict_single_ones, Data.structure, (29, 7)),
        ('bs', None,
         Data.predict_single_ones, Data.structure, (51, 7)),
        ('is', 'us-gaap:ProfitLoss',
         Data.predict_single_ones, Data.structure, (37, 7)),
        ('cf', 'us-gaap:Liabilities',
         Data.predict_single_ones, Data.structure, (0, 7)),
        ('buu', 'us-gaap:Liabilities',
         Data.predict_single_ones, Data.structure, (0, 7)),
        ('cf', None,
         Data.predict_single, {}, (0, 7))])
    def test_chapter_node_filter(
            self,
            start_chapter, start_tag,
            predict, structure, answer_shape):
        """
        test predict with start_chapter and start_tag
        """
        self.m.predict.side_effect = predict

        self.cl.leaf = True
        self.cl.start_chapter = start_chapter
        self.cl.start_tag = start_tag

        self.cl.predict_all(structure)
        #Data.predicted.append(self.cl.predicted.copy())

        self.assertEqual(self.cl.predicted.shape, answer_shape)

class TestModelFilter(unittest.TestCase):
    """
    test filtering by filter_model
    """
    @patch('indi.lstmmodels.Models')
    def setUp(self, mock_models):
        instance = mock_models.return_value

        self.m1 = mock.Mock()
        self.m2 = mock.Mock()
        instance.models = {'model1': self.m1,
                           'model2': self.m2}

        self.cl = CL.SingleOnlyChild(
            fdict='dictionary.csv',
            fmodel='model1',
            start_chapter='cf',
            start_tag=None,
            max_len=40,
            leaf=False)

        self.dep_cl = CL.MultiParentAndChild(
            fdict='dictionary.csv',
            fmodel='model2',
            start_chapter='cf',
            start_tag=None,
            max_len=40,
            leaf=False)
        self.dep_cl.filter_model = self.cl
        self.dep_cl.filter_id = 1

    @parameterized.expand([
        (Data.predict_single_ones, Data.predict_multi,
         Data.structure, (50, 7), 85),
        (Data.predict_single_zeros, Data.predict_multi,
         Data.structure, (50, 7), 0),
        (Data.predict_single, Data.predict_multi,
         Data.structure, (50, 7), 62),
        (Data.predict_single_ones, Data.predict_multi,
         {}, (0, 7), 0)
        ])
    def test(self, p1, p2, structure,
             answer_shape, answer_sum):
        """
        various test cases
        """

        self.m1.predict.side_effect = p1
        self.m2.predict.side_effect = p2

        self.cl.predict_all(structure)
        self.dep_cl.predict_all(structure)

        Data.predicted.append(self.dep_cl.predicted.copy())

        self.assertEqual(self.dep_cl.predicted.shape, answer_shape)
        self.assertEqual(self.dep_cl.predicted['class'].sum(), answer_sum)

class TestCreate(unittest.TestCase):
    """
    test indi.lstmclass.create procedure
    """
    @parameterized.expand([
        ('c', 1, 1, 20, 'dictionary.csv', CL.MultiOnlyChild),
        ('c', 1, 0, 20, 'dictionary.csv', CL.MultiOnlyChild),
        ('c', 1, 1, 30, 'dictionary.csv', CL.MultiOnlyChild),
        ('c', 0, 0, 50, 'dictionary.csv', CL.SingleOnlyChild),
        ('pc', 1, 1, 10, 'dictionary.csv', CL.MultiParentAndChild),
        ('pc', 0, 1, 20, 'dictionary.csv', CL.SingleParentAndChild),
        ('buu', 0, 1, 20, 'dictionary.csv', indi.exceptions.ModelException),
        ('pc', 2, 1, 20, 'dictionary.csv', indi.exceptions.ModelException)
        ])
    def test(self, pc, multi, leaf, max_len, fdict, expected):
        with unittest.mock.patch('indi.lstmmodels.Models') as mock_models:
            instance = mock_models.return_value

            m_1 = mock.Mock()
            m_2 = mock.Mock()
            instance.models = {'model1': m_1,
                               'model2': m_2}

            start_chapter = 'bs'
            start_tag = 'us-gaap:Liabilities'
            fmodel = 'model1'

            args = {'fdict': fdict,
                    'pc': pc,
                    'multi': multi,
                    'max_len': max_len,
                    'leaf': leaf,
                    'start_tag': start_tag,
                    'start_chapter': start_chapter}

            if issubclass(expected, Exception):
                with self.assertRaises(expected):
                    CL.create(fmodel, args)
            else:
                cl = CL.create(fmodel, args)
                self.assertTrue(isinstance(cl, expected))
                self.assertEqual(cl.max_len, max_len)
                self.assertEqual(cl.fmodel, fmodel)
                self.assertEqual(cl.start_chapter, start_chapter)
                self.assertEqual(cl.start_tag, start_tag)
                self.assertEqual(cl.leaf, (leaf == 1))


if __name__ == '__main__':
    unittest.main()
