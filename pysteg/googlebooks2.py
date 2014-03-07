#!/usr/bin/env python3

import re
import string

from itertools import count, chain
from functools import lru_cache

POS_TAGS = frozenset({"NOUN", "VERB", "ADJ", "ADV", "PRON", "DET", "ADP", "NUM",
        "CONJ", "PRT", ".", "X"})
        
@lru_cache(maxsize=1)
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
    
@lru_cache(maxsize=1)
def create_partition_order_index():
    """Returns a dictionary keyed by partition and valued by partition order."""
    return dict(zip(create_partition_names(),count(1)))
    
def partition_order(p):
    """Returns the order key of a partition."""
    return create_partition_order_index()[p]
    
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
        
def pos_tagged(word):
    """Returns if a word either is or contains a POS tag."""

    # A part-of-speech (POS) tag can either be appended to the end of a word, as
    # in "eat_VERB", or can be a placeholder on its own, as in "_VERB_".
    pos_pattern = re.compile("_[A-Z.]+_?$")
    
    for potential_tag in pos_pattern.findall(word.decode()):
        if potential_tag.strip("_") in POS_TAGS:
            return False
    return True

