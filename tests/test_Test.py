## @file Test.py
# @author weaves
# @brief Unittest of MInfo
#
# This module tests the ancillary operations and the 
# 
# @note
#
# Relatively complete test.

from weaves import singledispatch1

import sys, logging, os
from unidecode import unidecode

import unittest

logging.basicConfig(filename='test.log', level=logging.DEBUG)
logger = logging.getLogger('Test')
sh = logging.StreamHandler()
logger.addHandler(sh)

class Test0:

    @singledispatch1
    def add(self, a, b):
        raise NotImplementedError('Unsupported type')
 
    @add.register(int)
    def _(self, a, b):
        logger.debug("First argument is of type " + type(a).__name__)
        return a * b
        
    @add.register(str)
    def _(self, a, b):
        logger.debug("First argument is of type " + type(a).__name__)
        return "(\"{0:s}\", \"{1:s}\"".format(a, b)


## A test driver for GMus0
#
# @see GMus0
class Test(unittest.TestCase):
    """
    Test MInfo
    """
    test0 = None

    ## Sets pandas options and logging.
    @classmethod
    def setUpClass(cls):
        pass
    
    ## Logs out.
    @classmethod
    def tearDownClass(cls):
        pass

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
    def test_000(self):
        test0 = Test0()
        test0.add(1,2)
        test0.add("1", "2")
        return

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
        unittest.main(module='Test', verbosity=3, failfast=True, exit=False)
