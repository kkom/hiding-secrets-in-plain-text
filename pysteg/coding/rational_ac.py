from functools import lru_cache

from sympy import Rational

# Database of symbol relative frequencies and mappings between letters and 
# indices
frequencies = {' ': 1828, '.': 100, 'P': 150, 'Q': 8, 'R': 498, 'S': 531,
               'T': 751, 'U': 227, 'V': 79, 'W': 170, 'X': 14, 'Y': 142, 'Z': 5,
               'A': 653, 'B': 125, 'C': 223, 'D': 328, 'E': 1026, 'F': 198,
               'G': 162, 'H': 497, 'I': 566, 'J': 9, 'K': 56, 'L': 331,
               'M': 202, 'N': 571, 'O': 615}

indices = {' ': 26, '.': 27, 'P': 15, 'Q': 16, 'R': 17, 'S': 18, 'T': 19,
           'U': 20, 'V': 21, 'W': 22, 'X': 23, 'Y': 24, 'Z': 25, 'A': 0, 'B': 1,
           'C': 2, 'D': 3, 'E': 4, 'F': 5, 'G': 6, 'H': 7, 'I': 8, 'J': 9,
           'K': 10, 'L': 11, 'M': 12, 'N': 13, 'O': 14}

letters = {0: 'A', 1: 'B', 2: 'C', 3: 'D', 4: 'E', 5: 'F', 6: 'G', 7: 'H',
           8: 'I', 9: 'J', 10: 'K', 11: 'L', 12: 'M', 13: 'N', 14: 'O', 15: 'P',
           16: 'Q', 17: 'R', 18: 'S', 19: 'T', 20: 'U', 21: 'V', 22: 'W',
           23: 'X', 24: 'Y', 25: 'Z', 26: ' ', 27: '.'}

@lru_cache(maxsize=None)
def l(m):
    """Letter for an index."""
    return letters[m]

@lru_cache(maxsize=None)
def i(l):
    """Index for a letter."""
    return indices[l]

@lru_cache(maxsize=None)
def p(m):
    """Marginal probability of a symbol defined by its index."""
    @lru_cache()
    def total():
        return sum(frequencies.values())
    
    return Rational(frequencies[l(m)], total())

@lru_cache(maxsize=None)
def c(m):
    """Cumulative probability of a symbol defined by its index."""
    if m == 0:
        return Rational(0)
    else:
        return p(m-1) + c(m-1)

@lru_cache(maxsize=None)
def encode_rec(S):
    """
    Recursive definition of an arithmetic encoder. This might fail for long 
    streams.
    """
    if S == ():
        return (Rational(0), Rational(1))
    else:
        *init, last = S
        (b, l) = encode_rec(tuple(init))
        return (b + c(last) * l, p(last) * l)

@lru_cache(maxsize=None)     
def encode_iter(S):
    """
    Iterative definition of an arithmetic encoder.
    """
    (b, l) = (Rational(0), Rational(1))
    
    for s in S:
        (b, l) = (b + c(s) * l, p(s) * l)

    return (b, l)
