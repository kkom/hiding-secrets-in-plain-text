import collections
import functools

import sympy

from pysteg.coding.crypto import random_bits

Interval = collections.namedtuple("Interval", "b l")

__half = sympy.Rational(1,2)
__first_half = Interval(sympy.Rational(0), sympy.Rational(1,2))
__second_half = Interval(sympy.Rational(1,2), sympy.Rational(1,2))

def bit2interval(bit):
    """Convert 0 to the [0,0.5) interval and 1 to the [0.5,1) interval."""

    if bit == 0:
        return __first_half
    else:
        return __second_half

def bits2interval(bits):
    """Convert a sequence of bits to the interval they describe."""
    return functools.reduce(find_subinterval, map(bit2interval, bits))

def interval2bit(interval, mode):
    assert(mode in ("sub", "super"))

    if mode == "super":
        # The order of the first two conditions matters. Since the interval is
        # closed at start and open at the end, a zero-length interval at 1/2
        # should be matched to 1.
        if interval.b >= __half:
            return (1, scale_interval(__second_half, interval))
        elif interval.b + interval.l <= __half:
            return (0, scale_interval(__first_half, interval))
        else:
            return None
    else:
        bottom_distance = interval.b
        top_distance = 1 - (interval.b + interval.l)

        if bottom_distance <= 0 and top_distance <= 0:
            return None
        if top_distance < bottom_distance:
            return (1, scale_interval(__second_half, interval, subunit=False))
        else:
            return (0, scale_interval(__first_half, interval, subunit=False))

def interval2bits(interval, mode):
    """
    Convert an interval to a sequence of bits that describe its smallest
    superinterval or largest subinterval.
    """

    assert(mode in ("sub", "super"))

    bits = []

    next = interval2bit(interval, mode)
    while next is not None:
        bits.append(next[0])
        interval = next[1]
        next = interval2bit(interval, mode)

    return tuple(bits)

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

def is_subinterval(interval, subinterval, proper=False):
    if proper:
        return (subinterval.b > interval.b and
                subinterval.b + subinterval.l < interval.b + interval.l)
    else:
        return (subinterval.b >= interval.b and
                subinterval.b + subinterval.l <= interval.b + interval.l)

def random_interval(n, seed=None):
    """Randomly generate an interval of n bits."""
    return bits2interval(random_bits(n, seed))

def scale_interval(superinterval, interval, subunit=True):
    """Scale the interval as if the superinterval was [0,1)."""

    return create_interval(
        (interval.b - superinterval.b) / superinterval.l,
        interval.l / superinterval.l,
        subunit=subunit
    )
