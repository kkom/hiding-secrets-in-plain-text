import collections

import sympy

Interval = collections.namedtuple("Interval", "b l")

def create_interval(base, length, divisor=1, subunit=True):
    b = sympy.Rational(base, divisor)     # interval base
    l = sympy.Rational(length, divisor)   # interval length

    # Interval has to have a positive length
    assert(l > 0)

    if subunit:
        # And be a subinterval of [0,1)
        assert(b >= 0)
        assert(b < 1)
        assert(b+l <= 1)

    return Interval(b, l)

def find_subinterval(interval, ratio, subunit=True):
    """Return a subinterval of the interval according to the supplied ratio."""

    return create_interval(
        interval.b + ratio.b * interval.l,
        interval.l * ratio.l,
        subunit=subunit
    )

def scale_interval(superinterval, interval, subunit=True):
    """Scale the interval as if the superinterval was [0,1)."""

    return create_interval(
        (interval.b - superinterval.b) / superinterval.l,
        interval.l / superinterval.l,
        subunit=subunit
    )
