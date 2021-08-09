# @file POSet.py
# @brief Partially-ordered sets.
# @author weaves
#
# @details
# This class provides some generators for partially-ordered sets.
#
# @note:
# 

import logging
from itertools import permutations, combinations
from functools import partial, reduce
import scipy.special as scis

import numpy as np

# logging.basicConfig(filename='POSet.log', level=logging.DEBUG)
logger = logging.getLogger('POSet')
# sh = logging.StreamHandler()
# logger.addHandler(sh)


## Helper methods
# These methods are the ones being ported to Cython.

def prepend(s, parent):
    """
    Returns a new list that is a copy of parent with s as the first element.

    A copy and an insert make this the costly operation.
    """
    parent = parent.copy()
    parent.insert(0, s)
    return parent

def branches(h, s):
    """The branches are the combinations of the list h, prepended to s.

    This is a generator. Each yield returns a list that one of the combinations
    of the symbols in h, this is then prepended to s.
    """
    for y in combinations(h, len(h)-1):
        yield prepend(frozenset(y), s)

def fork(h, s):
    """Collects all the branches of the head h and the tail s and produces a path
    for each.

    This is a recursive algorithm.
    """
    return [ paths(y) for y in branches(h, s) ]

def paths(s):
    """
    Provides the paths within a sequence.

    If any element has more than element within it, then the combinations
    are enumerated.
    """
    h = s[0]
    if len(h) <= 1:
        return prepend(frozenset(), s)
    return fork(h, s)

def combine1(x, y, set0=set()):
    """
    Append a tuple of x and y to the set set0.

    @note:
    This is used as a partial
    """
    set0.add( (x,y) )
    return y


def remap(xs, d1=None):
    """
    Provide a tuple of dictionary look-ups.

    @type xs: iterable
    @param xs: sequence of keys in d1
    @type d1: dictionary
    @param d1: a dictionary
    @return:tuple of values
    """
    return ( d1[x] for x in xs )

def join0(x, s=None):
    """
    A join function that can be bound to a separator string s.

    @note:
    Is an identity function if used with s as None.
    """
    if s is None:
        return x
    return s.join(x)

def remapN(xs, remap0=remap):
    """
    Apply the remap0 function to each of xs.
    """
    for x in xs:
        yield remap0(x)

def sterm(n, k, j):
    """
    This is a numerical calculation of the base term of a Stirling number term.
    """
    return int((-1)**(k-j)*scis.binom(k,j)*j**n)

def stirling2_(n):
    """
    A sequence of counts that are the components of the Stirling Number of Second Kind.

    Number terms up to n.

    """
    for k in range(n+1):
        for j in range(k+1):
            yield sterm(n, k, j)

## End Helper Methods

class Impl(object):
    """
    Miscellaneous set and preference order theoretic methods.

    This is accessed via a Singleton known as POSetOps.
    """

    _logger = logging.getLogger('weaves')
    """Interface with logging"""

    _tions = None
    """Calculate permutations function"""

    set0 = set()
    """Highly used local mutable set."""

    def __init__(self, **kwargs):
        self._tions = lambda xs, n: permutations(xs, n)

    def unordered_Bell(self, n):
        """
        Number of partitions of a set of size n.

        This counts L{partitions}

        @note:
        Recursive formulation with the method defined in this function.
        """

        def ob(n0):
            if n0<=1:
                return 1
            return sum([ ob(k) * scis.comb(n0-1, k) for k in range(n0)])

        return int(ob(n))

    def ordered_Bell(self, n):
        """
        The ordered Bell number for a set of size n.

        This counts L{weak_orderings}

        @note:
        List-based evaluation.
        """
        return sum(stirling2_(n))

    def ordered_Bell1(self, n):
        """
        The ordered Bell number for a set of size n.

        @note:
        Recursive calculation.
        """
        if n<=1:
            return 1
        l0 =[ scis.binom(n, k-1)* self.ordered_Bell1(k-1) for k in range(n+1) ]

        return int(sum(l0))

    def strong(self, xs0):
        """
        A union of the strong orderings of all permutations.

        For a list of symbols, generates all the strong orderings from
        each permutation of the alphabets.

        @note:
        This is the author's own construction. It turns up surprisingly
        often.

        """
        s00 = set()
        for i in iter(range(len(xs0))):
            s00 = s00.union(self._tions(xs0, i+1))
        return s00

    def total_order(self, s0):
        """
        Total order, for a list of symbols, generates a total strong ordering.

        A000142 Factorial numbers: n! = 1*2*3*4*...*n (order of symmetric group
        S_n, number of permutations of n letters).

        """
        return permutations(s0)

    def pairs_(self, l0):
        """
        Given a list of symbols, form overlapping pairs.
        """
        s0 = set()
        combine2 = partial(combine1, set0=s0)
        reduce(combine2, l0);
        return s0

    def partial_order(self, s00):
        """Partial order

        A partial ordering of the symbols in the set. This isn't a a total
        ordering. It can be thought of as all the sub-strings of a string,
        ordered by sub-string. Partial orderings are unusual they are written
        because elements an be comparable or incomparable. And if comparable,
        they can be equal or less-than.

        So for a string ab: we have three partial orders. 

        One is the two isolated cases, a, b are not comparable, ( null < a),
        (null < b).

        Then the next case is (a < ab). And there is a variant (b < ab).

        This is an unusual sequence. I haven't found have a single recursive
        generator for this sequence. It is possible to use Hasse diagrams and
        generate the permutations and use those with the Hasse diagram.

        Moving on to abc. We have one partial ordering where none are
        comparable: null < a and null < b and null < c. This is one case.

        Then there is null < a < ab, and null < c on its own and there are 3
        sets of these, one for each of a, b and c. So 3 in all.

        (Ignoring the repeated null from here n.)

        Then a and b equally preferred, ab < abc and bc < abc and this has just
        3 variants.

        Then a < ab < abc is another ordering, and it has a variant a < ac <
        abc. And there are 3 sets of these, so another 6 orderings.

        And a final case is a == b == c

        null < a, b == c and 3 of these in total.

        1 + 6 + 3 + 6 + 3 = 19

        A001035 Number of partially ordered sets ("posets") with n labeled
        elements (or labeled acyclic transitive digraphs).

        """
        s0 = set(self.singletons_(s00))
        if len(s00) <= 1:
            return s0

        s1 = paths([frozenset(s00)])
        if len(s00) <= 2:
            logger.info("po: {}".format(s1))
            s2 = set( tuple(x) for x in self.as_(s1) )
            s2.add(tuple(s0))
            return s2

        s2 = [ self.pairs_(x) for x in self.as_(s1) ]  # calls a helper

        s3 = set().union(*s2)
        return s1

    def singletons_(self, s00):
        """
        The anti-chains form a partial ordering.
        """
        s0 = s00
        if not isinstance(s00, frozenset):
            s0 = frozenset(tuple(s00))
        return frozenset( ( ((), x) for x in s0 ) )

    def paths0_(self, s00):
        """
        Return sets of complete paths.
        """
        return paths([frozenset(s00)])

    def tupler0_(self, l):
        """
        Adds a list of tuples, as an element, to the object's working set, s0.

        Recursive with an exit of last element in list is frozenset.
        """
        if len(l) >= 1 and isinstance(l[-1], frozenset):
            self.set0.add(tuple(l))
            return

        for x in l:
            self.tupler0_(x)
        return


    def as_(self, l0, type0=set, type1=None):
        """
        A render method, converts the entity l0 to something of type type0.

        @note:
        currently only type is supported.
        """
        if type0 == set:
            self.set0.clear()
            self.tupler0_(list(l0))
            return self.set0

        return None

    def partitions(self, set_):
        """Set partitions

        A000110 Bell or exponential numbers: number of ways to partition a set
        of n labeled elements.

        @note: From the web. A Gray code variant. This can produce another set
        of sequences if set_ is a list.

        @author: Thomas Dybdahl Ahle

        """

        ctr0 = lambda: list()
        ext0 = lambda px, item: px.append(item)

        if type(set_) is set:
            ctr0 = lambda: set()
            ext0 = lambda px, item: px.add(item)

        def partitions_(set_):
            if not set_:
                yield []
                return

            for i in range(int(2**len(set_)/2)):
                parts = [ctr0(), ctr0()]
                for item in set_:
                    # parts[i&1].add(item)
                    ext0(parts[i&1], item)
                    i >>= 1
                for b in partitions_(parts[1]):
                    yield [parts[0]]+b

        if type(set_) is set:
            return partitions_(set_)

        ## for lists.
        sss = permutations(set_, len(set_))
        return [ s for s in [ list(partitions_(ss)) for ss in sss ] ]

    def syms2set(self, syms='ABC'):
        """
        Returns a string as a set.
        """
        s0 = syms
        if isinstance(syms, str):
            s0 = list(syms)
        s0 = s0.copy()
        if not isinstance(s0, set):
            s0 = set(syms)

        return s0

    def weak_orderings1(self, syms='ABC'):
        """
        Return weak orderings of set structured by set partition.

        Each tuple is a pair with its original set and ordered tuples.
        """
        s0 = self.syms2set(syms=syms)
        for x in self.partitions(s0):
            yield [x, tuple(permutations(x))]

    def weak_orderings(self, syms='ABC'):
        """
        Return weak orderings.

        Each tuple is a preference.
        """
        s0 = self.syms2set(syms=syms)

        for x in self.partitions(s0):
            for y in permutations(x):
                yield y

    def sym2basis(self, l0):
        """
        Convert a string of letters to a set of integers.

        @note:
        0 is used for a special function so we don't map to that.
        """
        if isinstance(l0, str):
            l0 = tuple(l0)

        if not isinstance(l0, tuple):
            raise ValueError('tuple expected')

        if not l0:
            return []
        return dict(zip(range(1, len(l0)+1), l0))

    def _counts(self, adjacency0=dict()):
        """
        Given a dictionary of lists, returns the key and the length.
        """
        return dict(iter(( (k, len(v)) for k, v in adjacency0.items() )))

    def _nodes(self, d0):
        """
        Returns all the permutations of the keys of a dictionary.

        @type d0: dictionary
        @param d0: usually the preference names.
        """
        l0 = d0.keys()
        # Node addresses n!
        l1 = permutations(l0, len(l0))
        return tuple(l1)

    def _faces(self, nodes, ndiff=2, nX=1):
        """

        """
        l1 = map(np.array, nodes)

        l2 = tuple(combinations(l1, 2))

        m0 = int(ndiff * nX)
        l3 = map(lambda a: m0 == sum(abs(a[0] - a[1])), l2)
        l4 = [i for i, x in enumerate(tuple(l3)) if x]
        edges = [l2[i] for i in l4]

        f0 = lambda x: ( tuple(np.ndarray.tolist(x[0])), tuple(np.ndarray.tolist(x[1])) )
        return tuple(map(f0, edges))

    def adjacency(self, d0, ndiff=2, nX=1):
        """
        Graph vertices for a permutohedron.

        @type d0: string or dictionary.
        @param d0: list of symbols.

        Find those that adjacent to one another.
        """
        # Map list to ordinal
        if not isinstance(d0, dict):
            d0 = self.sym2basis(d0)

        nodes = self._nodes(d0)
        edges1 = self._faces(nodes)

        return { 'm': d0, 'n': nodes, 'e': edges1 }

    def remap(self, adjacency0=None, map0=None, nodes0=None,
              edges0=None, sep0=">"):
        if not adjacency0 is None:
            map0=adjacency0['m']
            nodes0=adjacency0['n']
            edges0=adjacency0['e']

        if map0 is None:
            raise ValueError('map0 is required')

        remap1 = partial(remap, d1=map0)
        join1 = partial(join0, s=sep0)

        nodes1 = nodes0
        edges1 = edges0
        if not nodes1 is None:
            nodes1 = dict(iter([ (x, join1(remap1(x))) for x in nodes0 ]))
            if not edges1 is None:
                remap2 = partial(remap, d1=nodes1)
                edges1 = ( tuple(x) for x in remapN(edges0, remap0=remap2) )

        if adjacency0 is None:
            adjacency0 = { 'm': map0, 'n': nodes0, 'e': edges0 }
        d0 = { 'map': map0, 'nodes': nodes1, 'edges':edges1 }
        adjacency0.update(d0)
        return adjacency0

class Singleton(object):
    """
    Single instance of L{Impl} this provides access to the implementation.
    """
    _impl = None
    
    @classmethod
    def instance(cls):
        if cls._impl is None:
            cls._impl =  Impl()
        return cls._impl
