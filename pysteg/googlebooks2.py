#!/usr/bin/env python3

import string

from itertools import chain
from functools import lru_cache

def create_partition_names():
    """Returns a tuple of all valid partition names."""

    def gen_letter_subpartitions(letter):
        """Generates all proper letter subpartitions followed by a catch-all."""
        subpartitions = map(lambda c: letter + c, string.ascii_lowercase)
        return chain(subpartitions, letter)
    
    letter_partitions = map(gen_letter_subpartitions, string.ascii_lowercase)
    all_partitions = chain(string.digits, letter_partitions, ('_',));
    return tuple(chain.from_iterable(all_partitions))
    
@lru_cache(maxsize=1)
def create_partition_names_frozenset():
    return frozenset(create_partition_names())
    
def get_partition(word):
    """Returns partition to which a word corresponds."""
    
    partitions = create_partition_names_frozenset();
    prefix = word[:2].lower()
    
    if prefix in partitions:
        return prefix
    elif len(prefix) > 1 and prefix[0] in partitions:
        return prefix[0]
    else:
        return "_"
