## @file Test1.py
# @author weaves
# @brief unittest of my utilities
#
# This module tests the ancillary operations and the 
# 
# @note
#
# Relatively complete test.

from weaves import TimeOps
from weaves import POSetOps
from weaves import Utility
from weaves import __Id__ as weavesId

import string

import sys, logging, os
from unidecode import unidecode
import scipy.special as scis

from datetime import datetime, timezone, timedelta, date

from collections import Counter

import unittest

logging.basicConfig(filename='test.log', level=logging.DEBUG)
logger = logging.getLogger('Test')
sh = logging.StreamHandler()
logger.addHandler(sh)

## A test driver for GMus0
#
# @see GMus0
class Test1(unittest.TestCase):
    """
    Test
    """

    N = 6
    ss = None

    ## Null setup. Create a new one.
    def setUp(self):
        logger.info('setup')
        self.ss = map(lambda n: string.ascii_letters[0:n], range(1,self.N+1))
        return

    ## Null setup.
    def tearDown(self):
        logger.info('tearDown')
        return

    ## Loaded?
    ## Is utf-8 available as a filesystemencoding()
    def test_001(self):
        self.assertIsNotNone(weavesId)
        logger.info("module: Id: " + weavesId)
        return

    def test_005(self):
        s0 = TimeOps.instance().dofy(datetime.today())
        logger.info("dofy: " + str(s0))
        return

    def test_007(self):
        """
        How to get a datetime that is an advance 
        """
        adv = 1.525
        s0 = TimeOps.instance().dtadvance2(seconds=adv)
        logger.info("epoch: " + str(adv) + "; " + str(s0))
        return

    def test_010(self):
        """
        Unordered Bell 

        Total ordering: A000142
        """
        p0 = POSetOps.instance()

        for s in self.ss:
            n0 = int(scis.perm(len(s), len(s)))
            n1 = len(tuple(p0.total_order(s)))
            self.assertEqual(n0, n1)
            logger.info("total ordering: {}: {} == {}".format(len(s), n0, n1))


    def test_011(self):
        """
        Unordered Bell 

        Equivalence relation: A000110
        """
        p0 = POSetOps.instance()
        logger.info("unordered Bell: {}".format(p0.unordered_Bell(3)))

        for s in self.ss:
            n0 = p0.unordered_Bell(len(s))
            n1 = len(tuple(p0.partitions(set(s))))
            self.assertEqual(n0, n1)
            logger.info("eqivalence: {}: {} == {}".format(len(s), n0, n1))

    def test_013(self):
        """
        Ordered Bell 

        Total preorder: A000670
        """
        p0 = POSetOps.instance()
        logger.info("ordered Bell: {}".format(p0.ordered_Bell(3)))

        for s in self.ss:
            n0 = p0.ordered_Bell(len(s))
            n1 = len(tuple(p0.weak_orderings(syms=s)))
            self.assertEqual(n0, n1)
            logger.info("ordered Bell: {}: {} == {}".format(len(s), n0, n1))

    def test_015(self):
        """
        Partial order

        Partial order: A000670
        """
        p0 = POSetOps.instance()

        for s in self.ss:
            x0 = p0.partial_order(s)
            logger.info("partial: {} {:.80}".format(len(x0), str(x0)))

    def test_020(self):
        """
        Initialize the singleton with the proxy and logger.
        """
        url0 = "http://www.bt.com"
        v0 = Utility.instance().fetch(url=url0, file='weaves.cfg', logger=logger)
        self.assertIsNotNone(v0)
        Utility.logger().info(Utility.instance().take(20, v0))
        self.assertTrue(Utility.instance().isvalid0(url0, base=True))

    def test_022(self):
        url0 = "http://www.bt.com"
        v0 = Utility.instance().fetch(url=url0)
        self.assertIsNotNone(v0)
        logger.info(Utility.instance().take(20, v0))
        self.assertTrue(Utility.instance().isvalid0(url0, base=True))

#
# The sys.argv line will complain to you if you run it with ipython
# emacs. The ipython arguments are passed to unittest.main.

if __name__ == '__main__':
    if len(sys.argv) and "ipython" not in sys.argv[0]:
        # If this is not ipython, run as usual
        unittest.main(sys.argv)
    else:
        # If not remove the command-line arguments.
        sys.argv = [sys.argv[0]]
        unittest.main(module='Test1', verbosity=3, failfast=True, exit=False)
