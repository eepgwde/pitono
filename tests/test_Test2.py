## @file Test2.py
# @author weaves
# @brief Unittest of MInfo
#
# This module tests the ancillary operations and the 
# 
# @note
#
# Relatively complete test.

from weaves import POSetOps
from weaves.preferences import Pair0, Prefer0, NoPrefer0

import sys
import logging
import os
import string

import unittest

logging.basicConfig(filename='test.log', level=logging.DEBUG)
logger = logging.getLogger('Test')
sh = logging.StreamHandler()
logger.addHandler(sh)

## A test driver for POSetOps
class Test2(unittest.TestCase):
    """
    Test
    """

    ## Null setup. Create a new one.
    def setUp(self):
        logger.info('setup')
        return

    ## Null setup.
    def tearDown(self):
        logger.info('tearDown')
        return

    ## Loaded?
    ## Is utf-8 available as a filesystemencoding()
    def test_01(self):
        s0 = POSetOps.instance().strong("")
        logger.info("strong: " + str(s0))

    def test_03(self):
        s0 = POSetOps.instance().strong("ABCD")
        logger.info("strong: " + str(s0))

    def test_05(self):
        """
        Preferences
        """
        with self.assertRaises(ValueError) as context:
            Pair0('A', 'A')
        self.assertIs(type(context.exception),ValueError) 

        with self.assertRaises(ValueError) as context:
            Pair0('A', '')
        self.assertIs(type(context.exception),ValueError) 

        with self.assertRaises(ValueError) as context:
            Pair0('', 'B')
        self.assertIs(type(context.exception),ValueError) 

        r0 = Pair0('A', 'B')
        with self.assertRaises(ValueError) as context:
            Pair0(r0, r0)
        self.assertIs(type(context.exception),ValueError) 

        r0 = Pair0('A', 'B')
        r1 = Pair0('A', 'B')
        with self.assertRaises(ValueError) as context:
            Pair0(r0, r1)
        self.assertIs(type(context.exception),ValueError) 

        return

    def test_07(self):
        """
        Preferences
        """
        r0 = Pair0('A', 'B')
        p1 = Prefer0('A', 'B')
        np1 = NoPrefer0('A', 'B')
        self.assertTrue(p1 == np1)
        self.assertFalse(p1 != np1)

        self.assertFalse(p1.isKindOf(np1))
        self.assertTrue(p1.isKindOf(p1))

    def test_09(self):
        """
        Partitions
        """
        l0 = POSetOps.instance().partitions(set('ABC'))
        l0 = list(l0)
        logger.info("unordered: " + str(l0))

    def test_11(self):
        """
        Partitions
        """
        l0 = POSetOps.instance().partitions(list('ABC'))
        l0 = [ list(l) for l in list(l0) ]
        l1 = [ len(l) for l in list(l0) ]
        logger.info("ordered: {} {} {}".format(len(l0), l1, l0))

    def test_13(self):
        """
        weak orderings
        """
        l0 = POSetOps.instance().partitions(list('ABC'))
        l0 = [ list(l) for l in list(l0) ]
        l1 = [ len(l) for l in list(l0) ]
        logger.info("ordered: {} {} {}".format(len(l0), l1, l0))

    def test_23(self):
        """
        Nodes and partitions
        """
        d0 = POSetOps.instance().sym2basis('ABC')
        logger.info("dict: {}".format(d0))

    def test_25(self):
        """
        Nodes and partitions
        """
        d0 = POSetOps.instance().sym2basis('ABC')
        x0 = POSetOps.instance().adjacency(d0)
        logger.info("dict: {}".format(x0['m']))
        nodes = tuple(x0['n'])
        logger.info("nodes: {} {}".format(len(nodes), nodes))
        edges = tuple(x0['e'])
        logger.info("edges: {} {}".format(len(edges), edges))


    def test_27(self):
        """
        Nodes and partitions
        """
        d0 = POSetOps.instance().sym2basis('ABCD')
        x0 = POSetOps.instance().adjacency(d0)
        logger.info("dict: {}".format(x0['m']))
        nodes = tuple(x0['n'])
        logger.info("nodes: {} {}".format(len(nodes), nodes))
        edges = tuple(x0['e'])
        logger.info("edges: {} {}".format(len(edges), edges))

    def test_29(self):
        """
        Nodes and partitions
        """
        x0 = POSetOps.instance().adjacency('ABC')
        self.assertIsNotNone(x0)
        logger.info("dict: {}".format(x0['m']))
        nodes = tuple(x0['n'])
        logger.info("nodes: {} {}".format(len(nodes), nodes))
        edges = tuple(x0['e'])
        logger.info("edges: {} {}".format(len(edges), edges))

        x1 = POSetOps.instance().remap(adjacency0=x0)
        self.assertIsNotNone(x1)
        logger.info("keys: {}".format(x1.keys()))

        nodes = tuple(x1['nodes'].values())
        logger.info("nodes: {} {}".format(len(nodes), nodes))
        edges = tuple(x1['edges'])
        logger.info("edges: {} {}".format(len(edges), edges))

    def test_21(self):
        """
        Nodes and partitions
        """
        p0 = POSetOps.instance()
        x0 = p0.adjacency('ABC')
        self.assertIsNotNone(x0)
        logger.info("counts: {}".format(p0._counts(x0)))
        s0 = [ string.ascii_letters[0:n] for n in range(1,6) ]
        for s in s0:
            logger.info(p0._counts(p0.adjacency(s)))

    def test_23(self):
        """
        Nodes and partitions
        """
        p0 = POSetOps.instance()
        n = 4
        s0 = string.ascii_letters[0:n]
        a0 = p0.sym2basis(s0)
        logger.info(a0)
        nodes = p0._nodes(a0)
        logger.info(nodes)
        faces = p0._faces(nodes, ndiff=2, nX=1)
        logger.info(faces)
        logger.info("n: {}; nodes: {}; faces: {}".format(n, len(nodes), len(faces)))
        logger.info("n: {}; #faces: {}".format(n, 2 - (len(nodes) - len(faces))))

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
        unittest.main(module='Test2', verbosity=3, failfast=True, exit=False)
