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

def scale_interval(superinterval, interval):
    """Scale interval as if the superinterval was [0,1)."""

    return create_interval(
        (interval.b - superinterval.b) / superinterval.l,
        interval.l / superinterval.l
    )

def find_subinterval(interval, ratio):
    """Return a subinterval that is a ratio of a given interval."""

    return create_interval(
        interval.b + ratio.b * interval.l,
        interval.l * ratio.l
    )

def decode(next_fun, interval):
    """Decode an interval using the supplied next token function."""

    context = []
    search_result = next_fun(interval, tuple(context))

    while search_result is not None:
        print(search_result[0])
        context.append(search_result[0])
        search_result = next_fun(search_result[1], tuple(context))

    return context
