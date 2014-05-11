import collections

import sympy

Interval = collections.namedtuple("Interval", "b l")

def create_interval(base, length, divisor=1):
    b = sympy.Rational(base, divisor)     # interval base
    l = sympy.Rational(length, divisor)   # interval length

    # Interval has to be a subinterval of [0,1) and have a positive length
    assert(b >= 0)
    assert(b < 1)
    assert(l > 0)
    assert(b+l <= 1)

    return Interval(b, l)

def select_subinterval(interval, subinterval):
    """
    Return a subinterval of an interval.
    """
    return Interval(
        interval.b + subinterval.b * interval.l,
        interval.l * subinterval.l
    )
