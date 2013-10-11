"""
Shows the binary representation of a Python float in computer memory.
"""

import binascii
import re

def hex_to_bin(h):
    """
    Converts a hex string to the corresponding binary string. I.e. performs the
    F -> 1111, E -> 1110, etc. substitution.
    
    Based on: http://stackoverflow.com/a/1427846/907505
    """
    
    def byte_to_binary(n):
        return ''.join(str((n & (1 << i)) and 1) for i in reversed(range(8)))

    l = len(h)
    
    even_up = l % 2 == 1
    
    if even_up:
      h = h.rjust(l+1,'0')
    
    bin_str = ''.join(byte_to_binary(b) for b in binascii.unhexlify(h))
    
    if even_up:
      return bin_str[4:]
    else:
      return bin_str

def hexfloat_to_bin(h):
    """
    Returns an explicit binary representation of a number represented by a 
    floating point hexadecimal string.
    """
    
    hexfloat_pattern = re.compile(r"""
      (?P<sign>[\+\-])?             # sign as + or -
      0x                            # constant part of the representation
      (?P<integer>[0-9a-f]+)        # integer part written in hex
      (\.(?P<fraction>[0-9a-f]+))?  # fractional part written in hex
      (p(?P<exponent>[\+\-]\d+))?   # binary exponent written in decimal
    """, re.X | re.I)    
                              
    hexfloat_representation = hexfloat_pattern.match(h)
        
    i = hex_to_bin(hexfloat_representation.group('integer'))
    f = hex_to_bin(hexfloat_representation.group('fraction'))
    e = int(hexfloat_representation.group('exponent'))
    
    full = i + f
    
    point = len(i) + e
    
    if point < 0:
      full = ''.join(['0'] * abs(point)) + full
      point = 0
    
    return '{i}.{f}'.format(i=full[:point], f=full[point:]).strip('0')

def num_to_bin(x):
    """
    Return an explicit binary representation of any number.
    """ 
    
    f = float(x)
    
    return hexfloat_to_bin(f.hex())
    