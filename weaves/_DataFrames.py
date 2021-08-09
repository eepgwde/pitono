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

import logging

import os
import sys
import platform

## Assume that pandas has been loaded.
import pandas as pd

from functools import singledispatchmethod, update_wrapper

import configparser 

from cached_property import cached_property

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

    def log(self, msg):
        self.logger.info("logging: " + msg)

    @singledispatchmethod
    def merge(self, arg, name0=None):
        if arg is None:
            self.f0 = pd.DataFrame()
            return self.f0

        raise NotImplementedError("Cannot merge a")

    @merge.register
    def _(self, arg: pd.Series, name0=None):
        if name0 is not None:
            arg.name = name0
        self.logger.info("series")
        self.f0 = pd.concat([self.f0, arg], axis=1)
        return self.f0

    @merge.register
    def _(self, arg: pd.DataFrame, name0=None):
        self.logger.info("dataframe")
        self.f0 = pd.concat([self.f0, arg], axis=1)
        return self.f0

    def quality0(self, df, **kwargs):
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

        return self.f0

class Singleton(object):
    """
    Singleton for L{Impl}, this is known as TimeOps or Utility
    """
    _impl = None
    
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
