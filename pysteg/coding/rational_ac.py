import collections

import sympy

Interval = collections.namedtuple("Interval", "b l")

def create_interval(base_count, length_count, all_counts=1):
    return Interval(
        sympy.Rational(base_count, all_counts),
        sympy.Rational(length_count, all_counts)
    )

def select_subinterval(interval, subinterval):
    """
    Return a subinterval of an interval.
    """
    return Interval(
        interval.b + subinterval.b * interval.l,
        interval.l * subinterval.l
    )
