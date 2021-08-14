# @file Test4.py
# @author weaves
# @brief Unittest of DataFrames
#
# This module tests the basic operations
# 
# @note
#
# Relatively complete test.

from weaves import DataFrames, __version__

import sys
import logging
import os
import string

import pandas as pd

import unittest

logging.basicConfig(filename='test.log', level=logging.DEBUG)
logger = logging.getLogger('Test')
sh = logging.StreamHandler()
logger.addHandler(sh)

f1 = pd.read_csv("cache/gdp.csv")

## A test driver for DataFrames
class Test4(unittest.TestCase):
    """
    Test
    """

    f1 = None

    ## Null setup. Create a new one.
    def setUp(self):
        logger.info('setup ' + __version__)
        self.f1 = f1
        return

    ## Null setup.
    def tearDown(self):
        logger.info('tearDown')
        return

    ## Is utf-8 available as a filesystemencoding()
    def test_01(self):
        '''
        This passes the logger down to the singleton.
        '''
        s0 = DataFrames.instance(logger=logger)
        logger.info("type: " + type(s0).__name__)
        s0.log("not the root logger")

    ## Is utf-8 available as a filesystemencoding()
    def test_03(self):
        s0 = DataFrames.instance()
        logger.info("type: " + type(s0).__name__)
        s0.log("this is the root logger")

    ## Is utf-8 available as a filesystemencoding()
    def test_05(self):
        self.s0 = DataFrames.instance()
        f1 = self.s0.merge(pd.DataFrame())
        print(f1)

    ## Is utf-8 available as a filesystemencoding()
    def test_07(self):
        self.s0 = DataFrames.instance()
        f2 = self.s0.merge(self.f1)
        print(f2)

    ## Is utf-8 available as a filesystemencoding()
    def test_09(self):
        self.s0 = DataFrames.instance()

        obs = self.f1.shape[0]

        types = self.f1.dtypes
        counts = self.f1.apply(lambda x: x.count())
        distincts = self.f1.apply(lambda x: x.unique().shape[0])
        nulls = self.f1.apply(lambda x: x.isnull().sum())
        missingP = (self.f1.isnull().sum()/ obs)

        skewness = self.f1.skew()
        kurtosis = self.f1.kurtosis()

        uniques = self.f1.apply(lambda x: [x.unique()])
        uniques = uniques.transpose()
        uniques.columns = [ "uniques" ] 

        f2 = self.s0.merge(None)
        print(f2)

        f2 = self.s0.merge(types, name0='types')
        f2 = self.s0.merge(counts, name0='counts')
        f2 = self.s0.merge(distincts, name0='distincts')
        f2 = self.s0.merge(nulls, name0='nulls')
        f2 = self.s0.merge(missingP, name0='missingP')
        f2 = self.s0.merge(skewness, name0='skewness')
        f2 = self.s0.merge(kurtosis, name0='kurtosis')
        f2 = self.s0.merge(uniques, name0='uniques')
        print(f2)

    def test_11(self):
        f2 = DataFrames.instance().quality0(self.f1)
        print(f2)

    def test_13(self):
        f2 = DataFrames.instance().quality0(self.f1, predictor='Value')
        print(f2)

    def test_15(self):
        f2 = DataFrames.instance().qfetcher("http://localhost:4444/")
        print(type(f2))
        df = f2.fetch( r'select count i from trns')
        print(type(df))
        print(df)

    def test_17(self):
        f2 = DataFrames.instance().qfetcher("http://walti.mooo.com:4444/")
        print(type(f2))
        df = f2.fetch( r'select[10] from trns')
        print(type(df))
        print(df)


## sys.argv
# The sys.argv line will complain to you if you run it with ipython
# emacs. The ipython arguments are passed to unittest.main.

if __name__ == '__main__':
    if len(sys.argv) and "ipython" not in sys.argv[0]:
        # If this is not ipython, run as usual
        unittest.main(sys.argv)
    else:
        # If not remove the command-line arguments.
        sys.argv = [sys.argv[0]]
        unittest.main(module='Test3', verbosity=3, failfast=True, exit=False)
