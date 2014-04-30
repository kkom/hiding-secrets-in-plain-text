"""
Shows the binary representation of a Python float in computer memory.
"""

import binascii
import re

from bitstring import BitArray

def hexfloat_to_bin(h):
    """
    Returns an explicit binary representation of a number represented by a
    floating point hexadecimal string.
    """

    hexfloat_pattern = re.compile(r"""
      (?P<sign>[\+\-])?             # sign as + or -
      0x                            # the hexadecimal prefix
      (?P<integer>[0-9a-f]+)        # integer part written in hex
      (\.(?P<fraction>[0-9a-f]+))?  # fractional part written in hex
      (p(?P<exponent>[\+\-]\d+))?   # binary exponent written in decimal
    """, re.X | re.I)

    hexfloat_representation = hexfloat_pattern.match(h)

    s = hexfloat_representation.group('sign')
    i = BitArray('0x' + hexfloat_representation.group('integer')).bin
    f = BitArray('0x' + hexfloat_representation.group('fraction')).bin
    e = int(hexfloat_representation.group('exponent'))

    full = i + f

    point = len(i) + e

    if point < 0:
      full = ''.join(['0'] * abs(point)) + full
      point = 0

    b = '{i}.{f}'.format(i=full[:point], f=full[point:]).strip('0')

    if s:
        b = s + b

    return b

def num_to_bin(x):
    """
    Return an explicit binary representation of any number.
    """

    f = float(x)

    return hexfloat_to_bin(f.hex())
