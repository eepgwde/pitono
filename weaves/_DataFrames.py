## @file _DataFrames.py
# @brief Extensions to Python class and object dispatch.
# @author weaves
#
# @details
# This class provides classes and methods for Pandas support.
#
# @note
#
# The Singleton does nothing just noww.

from ._version import __version__, __Id__

import logging

import os
import sys
import platform

from urllib.parse import urljoin, quote

## Assume that pandas has been loaded.
import pandas as pd

# singledispatchmethod is not available to Python 3.7
from functools import singledispatch, update_wrapper

import configparser 

from cached_property import cached_property

class QFetcher(object):
    base0 = None
    url1 = '/a.csv?'

    def __init__(self, base0):
        self.base0 = base0

    def fetch(self, query0):
        self.query0 = query0
        url0 = quote(self.url1 + self.query0)
        url1 = urljoin(self.base0, url0)
        return pd.read_csv(url1)

class _Impl(object):
    """Many utility methods and features hidden behind a singleton.

    Features: Global configuration. A global debug logger.

    Methods: Date and time methods. An HTTP fetch() that can use an Agent
      header and a proxy server (needs a configuration file). Many functional
      programming constructs.

    """

    logger = None

    f0 = pd.DataFrame()

    """Local logger."""

    def __init__(self, **kwargs):
        """
        Initialization for the DataFrames singleton.
        """
        l0 = logging.getLogger()
        l0.addHandler(logging.NullHandler())
        self.logger = kwargs.get('logger', l0)

        self.merge = singledispatch(self.merge)
        self.merge.register(pd.Series, self._mergeSeries)
        self.merge.register(pd.DataFrame, self._mergeDataFrame)

    def qfetcher(self, base0):
        return QFetcher(base0)

    def log(self, msg):
        self.logger.info("logging: " + msg)

    def merge(self, arg, name0=None):
        """
        A set of methods for appending Pandas objects to a dataframe.

        The dataframe is held in the singleton. To re-initialize it, *arg*
        should be passed as None.

        After that, a Series or a DataFrame can be appended.
        """
        if arg is None:
            self.logger.info("merge: reset")
            self.f0 = pd.DataFrame()
            return self.f0

        raise NotImplementedError("Cannot merge a")

    def _mergeSeries(self, arg, name0=None):
        """
        This method appends a series, the Series needs a name to produce a
        column for it.
        """
        if name0 is not None:
            arg.name = name0
        self.logger.info("series")
        self.f0 = pd.concat([self.f0, arg], axis=1)
        return self.f0

    def _mergeDataFrame(self, arg, name0=None):
        """
        This method appends a Dataframe, name0 is not used in this method.
        """
        self.logger.info("dataframe")
        self.f0 = pd.concat([self.f0, arg], axis=1)
        return self.f0

    def quality0(self, df, **kwargs):
        """
        Generate a quality report for a dataframe.

        It generates and collates a set of reports.

        The kwargs argument can be used to pass *predictor* the name
        of a column to which the other columns will report a correlations.
        """
        obs = df.shape[0]

        types = df.dtypes
        counts = df.apply(lambda x: x.count())
        distincts = df.apply(lambda x: x.unique().shape[0])
        nulls = df.apply(lambda x: x.isnull().sum())
        missingP = df.isnull().sum()/obs

        skewness = df.skew()
        kurtosis = df.kurtosis()

        uniques = df.apply(lambda x: [x.unique()])
        uniques = uniques.transpose()
        uniques.columns = [ "uniques" ] 

        self.merge(None)
        self.merge(types, name0='types')
        self.merge(counts, name0='counts')
        self.merge(distincts, name0='distincts')
        self.merge(nulls, name0='nulls')
        self.merge(missingP, name0='missingP')
        self.merge(skewness, name0='skewness')
        self.merge(kurtosis, name0='kurtosis')
        self.merge(uniques, name0='uniques')
        if not df.columns.empty:
            self.merge(df.describe().transpose())

        p0 = kwargs.get('predictor', None)
        if p0 is not None:
            corr0 = df.corr()[p0]
            self.merge(corr0, name0='corr')

        return self.f0

    def map0(self, series0, map0=None):
        '''
        Remapped a column
        '''
        if map0 is None:
            map0 = series0.unique()
            map0 = dict(zip(map0, range(len(map0))))

        return series0.map(map0)

class Singleton(object):
    """
    Singleton for L{Impl}, this is known as TimeOps or Utility
    """
    _impl = None

    __version__ = __version__
    
    @classmethod
    def instance(cls, **kwargs):
        if cls._impl is None:
            cls._impl = _Impl(**kwargs)
        return cls._impl

    @classmethod
    def logger(cls, **kwargs):
        if cls._impl is None:
            cls._impl = _Impl(**kwargs)
        return cls._impl.logger
