## @file _Functors.py
# @brief Extensions to Python class and object dispatch.
# @author weaves
#
# @details
# This class provides some decorators for classes.
# And a Singleton.
#
# @note
#
# The Singleton includes a implementation that does some date arithmetic.

import logging
from datetime import datetime, date, timedelta
from tempfile import NamedTemporaryFile

import os
import sys
import platform
import getpass

## Assume that pandas has been loaded.
import pandas as pd

from functools import singledispatch, update_wrapper
from itertools import islice, chain, starmap, tee, zip_longest, cycle

from collections import deque

from urllib import request
from urllib.parse import urlparse

import configparser 

from cached_property import cached_property

def singledispatch1(func):
    """
    This provides a method attribution that allows a single dispatch from an
    object.
    """
    dispatcher = singledispatch(func)
    def wrapper(*args, **kw):
        return dispatcher.dispatch(args[1].__class__)(*args, **kw)
    wrapper.register = dispatcher.register
    update_wrapper(wrapper, dispatcher)
    return wrapper

class _Impl(object):
    """Many utility methods and features hidden behind a singleton.

    Features: Global configuration. A global debug logger.

    Methods: Date and time methods. An HTTP fetch() that can use an Agent
      header and a proxy server (needs a configuration file). Many functional
      programming constructs.

    """
    epoch = datetime.utcfromtimestamp(0)
    """The timestamp at 0 milliseconds."""

    _hrfmt = "{0:02d}:{1:02d}:{2:02d}.{3:03d}"
    """Hours, minutes, and seconds and milliseconds format."""

    logger = None
    """Local logger."""

    _config = None
    """A configparser object."""

    _opener = None
    """Header used by fetch method."""

    def __init__(self, **kwargs):
        """
        Creates a configuration object and a logger and invokes config()

        A named argument config can pass a configuration object.
        Similarly, a named logger can be passed.
        """
        self._config = kwargs.get('config', configparser.ConfigParser())
        self.logger = kwargs.get('logger', logging.getLogger('Test'))

        self.config(**kwargs)

    def config(self, **kwargs):
        """
        This is called by the constructor and initializes the config object.

        Takes 'file' as a named argument.
        """
        if 'file' in kwargs:
            self._config.read(kwargs['file'])

        if 'fetcher-proxy' in self._config.sections():
            d0 = self._config['fetcher-proxy']
            if d0['type'].startswith("socks5"):
                socks.set_default_proxy(socks.SOCKS5, d0['host'], int(d0['port']))
                socket.socket = socks.socksocket
            if 'hdrs' in d0:
                self._hdrs = ast.literal_eval(d0['hdrs'])
                self._opener = request.build_opener()
                self._opener.addheaders = self._hdrs

    ## Configured methods

    def fetch(self, **kwargs):
        """
        Fetch a URL using a configured proxy client.

        Pass 'url' as a named argument.
        """
        if self._opener is not None:
            request.install_opener(self._opener)
        return request.urlopen(kwargs['url'])

    ## Time utilities.

    def tm2dt(self, tm):
        """
        Converts a timestamp to a datetime by appending it to the epoch start-time.
        """
        return datetime.combine(self.epoch, tm)

    @cached_property
    def zulu(self):
        """Zero time"""
        return self.dt2tm1(self.epoch)

    def dtadvance(self, dt, tm):
        """
        Advance a date by a time.
        """
        return dt + (self.tm2dt(tm) - self.epoch)

    def dofy(self, dt):
        """
        Day of year, indexed from zero.
        """
        d = dt.date() if isinstance(dt, datetime) else dt
        return d.toordinal() - date(d.year, 1, 1).toordinal()

    def dt2tm1(self, d):
        """Returns the time as a string."""
        hr0 = self.dofy(d) * 24 + d.hour
        return self._hrfmt.format(hr0, d.minute,
                                  d.second, int(d.microsecond / 1000))

    def dtadvance2(self, **kwargs):
        """Advances the time by a delta."""
        dt = self.epoch
        return dt + timedelta(**kwargs)

    def isvalid0(self, url, **kwargs):
        """
        Utility boolean test method to check is a string is a URI.
        """
        try:
            result = urlparse(url)
            if 'base' in kwargs:
                if kwargs['base']:
                    return all([result.scheme, result.netloc])

            return all([result.scheme, result.netloc, result.path])
        except:
            return False

        return True

    def qparts(self, url, **kwargs):
        """
        Extract the query part of a URL and return it as a dictionary.

        The keywords allow a conversion function to be used: fconv=int
        """
        purl = url
        if self.isvalid0(url):
            purl = urlparse(url).query

        qs = dict([ x.split('=') for x in purl.split('&') ])
        if 'fconv' in kwargs:
            fconv = kwargs['fconv']
            qs = dict([ (x[0], fconv(x[1])) for x in qs.items() ])

        return qs

    neo4j_ports = None

    def neo4j(self, **kwargs):
        r0 = None

        if 'file' in kwargs:
            fname = kwargs.get('file')
            if fname is None:
                fname = os.path.expanduser('~/.neo4j/ports.csv')
            fname = os.path.expanduser(fname)
            if not(os.path.exists(fname)):
                return None
            self.neo4j_ports = pd.read_csv(fname)
            r0 = self.neo4j_ports

        if 'url' in kwargs:
            d0 = self._neo4j(kwargs.get('url'))
            if d0 is None:
                return None
            r0 = "{protocol}://{host}:{port}".format(**d0)

        if 'auth' in kwargs:
            d0 = kwargs.get('auth')
            d0['host'] = d0.get('host', getpass.getuser())
            d0['port'] = -1
            d0['protocol'] = 'auth'
            d1 = self._neo4j(d0)
            if d1 is None:
                return None
            r0 = ( d1['host'], d1['server'] )

        return r0

    def _neo4j(self, d0):
        r0 = None
        ddef = { 'host': platform.node(), 'protocol': 'bolt', 'port': 7687 }
        d0 = { **ddef, **d0 }
        df = self.neo4j_ports
        # we only usually want the port number
        if d0['port'] < 0:
            df1 = df[(df['host']==d0['host']) & (df['protocol']==d0['protocol']) & (df['port'] == d0['port']) ]
        else:
            df1 = df[(df['host']==d0['host']) & (df['protocol']==d0['protocol']) & (df['server']==d0['server'])]
        if df1.empty:
            return None
        return df1.iloc[0].to_dict()

    ## From https://docs.python.org/3/library/itertools.html

    def take(self, n, iterable):
        "Return first n items of the iterable as a list"
        return list(islice(iterable, n))

    def prepend(self, value, iterator):
        "Prepend a single value in front of an iterator"
        # prepend(1, [2, 3, 4]) -> 1 2 3 4
        return chain([value], iterator)

    def tabulate(self, function, start=0):
        "Return function(0), function(1), ..."
        return map(function, count(start))

    def tail(self, n, iterable):
        "Return an iterator over the last n items"
        # tail(3, 'ABCDEFG') --> E F G
        return iter(collections.deque(iterable, maxlen=n))

    def consume(self, iterator, n=None):
        "Advance the iterator n-steps ahead. If n is None, consume entirely."
        # Use functions that consume iterators at C speed.
        if n is None:
            # feed the entire iterator into a zero-length deque
            deque(iterator, maxlen=0)
        else:
            # advance to the empty slice starting at position n
            next(islice(iterator, n, n), None)

    def nth(self, iterable, n, default=None):
        "Returns the nth item or a default value"
        return next(islice(iterable, n, None), default)

    def all_equal(self, iterable):
        "Returns True if all the elements are equal to each other"
        g = groupby(iterable)
        return next(g, True) and not next(g, False)

    def quantify(self, iterable, pred=bool):
        "Count how many times the predicate is true"
        return sum(map(pred, iterable))

    def padnone(self, iterable):
        """Returns the sequence elements and then returns None indefinitely.

        Useful for emulating the behavior of the built-in map() function.
        """
        return chain(iterable, repeat(None))

    def ncycles(self, iterable, n):
        "Returns the sequence elements n times"
        return chain.from_iterable(repeat(tuple(iterable), n))

    def dotproduct(self, vec1, vec2):
        return sum(map(operator.mul, vec1, vec2))

    def flatten(self, listOfLists):
        "Flatten one level of nesting"
        return chain.from_iterable(listOfLists)

    def repeatfunc(self, func, times=None, *args):
        """Repeat calls to func with specified arguments.

        Example:  repeatfunc(random.random)
        """
        if times is None:
            return starmap(func, repeat(args))
        return starmap(func, repeat(args, times))

    def pairwise(self, iterable):
        "s -> (s0,s1), (s1,s2), (s2, s3), ..."
        a, b = tee(iterable)
        next(b, None)
        return zip(a, b)

    def grouper(self, iterable, n, fillvalue=None):
        "Collect data into fixed-length chunks or blocks"
        # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx"
        args = [iter(iterable)] * n
        return zip_longest(*args, fillvalue=fillvalue)

    def roundrobin(self, *iterables):
        "roundrobin('ABC', 'D', 'EF') --> A D E B F C"
        # Recipe credited to George Sakkis
        num_active = len(iterables)
        nexts = cycle(iter(it).__next__ for it in iterables)
        while num_active:
            try:
                for next in nexts:
                    yield next()
            except StopIteration:
                # Remove the iterator we just exhausted from the cycle.
                num_active -= 1
                nexts = cycle(islice(nexts, num_active))

    def partition(self, pred, iterable):
        'Use a predicate to partition entries into false entries and true entries'
        # partition(is_odd, range(10)) --> 0 2 4 6 8   and  1 3 5 7 9
        t1, t2 = tee(iterable)
        return filterfalse(pred, t1), filter(pred, t2)

    def powerset(self, iterable):
        "powerset([1,2,3]) --> () (1,) (2,) (3,) (1,2) (1,3) (2,3) (1,2,3)"
        s = list(iterable)
        return chain.from_iterable(combinations(s, r) for r in range(len(s)+1))

    def unique_everseen(self, iterable, key=None):
        "List unique elements, preserving order. Remember all elements ever seen."
        # unique_everseen('AAAABBBCCDAABBB') --> A B C D
        # unique_everseen('ABBCcAD', str.lower) --> A B C D
        seen = set()
        seen_add = seen.add
        if key is None:
            for element in filterfalse(seen.__contains__, iterable):
                seen_add(element)
                yield element
        else:
            for element in iterable:
                k = key(element)
                if k not in seen:
                    seen_add(k)
                    yield element

    def unique_justseen(self, iterable, key=None):
        "List unique elements, preserving order. Remember only the element just seen."
        # unique_justseen('AAAABBBCCDAABBB') --> A B C D A B
        # unique_justseen('ABBCcAD', str.lower) --> A B C A D
        return map(next, map(itemgetter(1), groupby(iterable, key)))

    def iter_except(self, func, exception, first=None):
        """ Call a function repeatedly until an exception is raised.

        Converts a call-until-exception interface to an iterator interface.
        Like builtins.iter(func, sentinel) but uses an exception instead
        of a sentinel to end the loop.

        Examples:
            iter_except(functools.partial(heappop, h), IndexError)   # priority queue iterator
            iter_except(d.popitem, KeyError)                         # non-blocking dict iterator
            iter_except(d.popleft, IndexError)                       # non-blocking deque iterator
            iter_except(q.get_nowait, Queue.Empty)                   # loop over a producer Queue
            iter_except(s.pop, KeyError)                             # non-blocking set iterator

        """
        try:
            if first is not None:
                yield first()            # For database APIs needing an initial cast to db.first()
            while True:
                yield func()
        except exception:
            pass

    def first_true(self, iterable, default=False, pred=None):
        """Returns the first true value in the iterable.

        If no true value is found, returns *default*

        If *pred* is not None, returns the first item
        for which pred(item) is true.

        """
        # first_true([a,b,c], x) --> a or b or c or x
        # first_true([a,b], x, f) --> a if f(a) else b if f(b) else x
        return next(filter(pred, iterable), default)

    def random_product(self, *args, **kwds):
        "Random selection from itertools.product(*args, **kwds)"
        repeat = 2
        pools = [tuple(pool) for pool in args] * repeat
        return tuple(random.choice(pool) for pool in pools)

    def random_permutation(self, iterable, r=None):
        "Random selection from itertools.permutations(iterable, r)"
        pool = tuple(iterable)
        r = len(pool) if r is None else r
        return tuple(random.sample(pool, r))

    def random_combination(self, iterable, r):
        "Random selection from itertools.combinations(iterable, r)"
        pool = tuple(iterable)
        n = len(pool)
        indices = sorted(random.sample(range(n), r))
        return tuple(pool[i] for i in indices)

    def random_combination_with_replacement(self, iterable, r):
        "Random selection from itertools.combinations_with_replacement(iterable, r)"
        pool = tuple(iterable)
        n = len(pool)
        indices = sorted(random.randrange(n) for i in range(r))
        return tuple(pool[i] for i in indices)

    def nth_combination(self, iterable, r, index):
        'Equivalent to list(combinations(iterable, r))[index]'
        pool = tuple(iterable)
        n = len(pool)
        if r < 0 or r > n:
            raise ValueError
        c = 1
        k = min(r, n-r)
        for i in range(1, k+1):
            c = c * (n - k + i) // i
        if index < 0:
            index += c
        if index < 0 or index >= c:
            raise IndexError
        result = []
        while r:
            c, n, r = c*r//n, n-1, r-1
            while index >= c:
                index -= c
                c, n = c*(n-r)//n, n-1
                result.append(pool[-1-n])
        return tuple(result)

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
