import collections
import functools

import sympy

from pysteg.coding.crypto import random_bits

Interval = collections.namedtuple("Interval", "b l")

# Rational numbers
__half = sympy.Rational(1,2)
__one = sympy.Rational(1)

# Half-unit intervals - [0, 1/2) for 0 and [1/2, 1/2) for 1
__first_half = Interval(sympy.Rational(0), sympy.Rational(1,2))
__second_half = Interval(sympy.Rational(1,2), sympy.Rational(1,2))

def bit2interval(bit):
    """Convert 0 and 1 to their half-unit intervals."""

    assert(bit in (0,1))

    if bit == 0:
        return __first_half
    else:
        return __second_half

def bits2interval(bits):
    """Convert a sequence of bits to the interval they describe."""
    return functools.reduce(select_subinterval, map(bit2interval, bits))

def interval2bit(interval, mode):
    """
    In "super" mode, return bit corresponding to the half-unit interval which is
    a superinterval of the input interval. Additionally, return as a second
    element of the tuple ratio of the input interval to the chosen half-unit
    interval. Return None if no half-unit interval satisfies this requirement.

    In "sub" mode, return whichever half-unit interval contains more of the
    input interval and the same ratio. Return None if the input interval is a
    superinterval of [0,1).
    """

    assert(mode in ("sub", "super"))

    if mode == "super":
        # Since all intervals are closed at start and open at the end, [1/2, 0)
        # i.e. a point at 1/2 should be matched to the [1/2, 1/2) interval. This
        # is why the order of the conditions matters.
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

def create_interval(base, length, divisor=__one, subunit=True):
    """
    Create an interval. Make sure that its length is positive. In most cases the
    interval should be a sub-unit interval, i.e. a subinterval of [0,1). This is
    enforced by default.
    """

    b = sympy.Rational(base, divisor)     # interval base
    l = sympy.Rational(length, divisor)   # interval length

    # Interval has to have a positive length
    assert(l > 0)

    # And be a subinterval of [0,1)
    if subunit:
        assert(b >= 0)
        assert(b < 1)
        assert(b+l <= 1)

    return Interval(b, l)

def select_subinterval(interval, ratio, subunit=True):
    """Return a subinterval of the given interval according to the ratio."""

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
    """Randomly generate a sub-unit interval corresponding to n bits."""
    return bits2interval(random_bits(n, seed))

def scale_interval(superinterval, interval, subunit=True):
    """Scale the interval as if the superinterval was [0,1)."""

    return create_interval(
        (interval.b - superinterval.b) / superinterval.l,
        interval.l / superinterval.l,
        subunit=subunit
    )
