import collections

import sympy

Interval = collections.namedtuple("Interval", "b l")

def create_interval(base_count, length_count, all_counts=1):
    return Interval(
        sympy.Rational(base_count, all_counts),
        sympy.Rational(length_count, all_counts)
    )

def select_subinterval(outer, inner):
    """
    Return a subinterval of the outer interval defined by the inner interval.
    """
    return Interval(
        outer.b + inner.b * outer.l,
        outer.l * inner.l
    )
