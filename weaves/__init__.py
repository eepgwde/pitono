# -*- coding: utf-8 -*-

from ._version import __version__, __Id__
from ._Functors import singledispatch1

from ._Functors import Singleton as TimeOps
from ._Functors import Singleton as Utility

from ._POSet import Singleton as POSetOps
from ._MCast import Singleton as MCast
from ._MCast import Enqueue
from ._MCast import StoppableThread

from ._DataFrames import Singleton as DataFrames

__copyright__ = 'Copyright 2016 Walter Eaves'
__license__ = 'GPLv3'
__title__ = 'weaves'

# appease flake8: the imports are purposeful
(__version__)
(__Id__)
