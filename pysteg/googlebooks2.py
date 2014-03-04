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
    
    if len(word) == 1:
        if word in partitions:
            return word
        else:
            return "_"
    else:
        if word[0:2] in partitions:
            return word[0:2]
        elif word[0:1] in partitions:
            return word[0]
        else:
            return "_"
            