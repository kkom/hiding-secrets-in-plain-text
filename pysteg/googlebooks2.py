#!/usr/bin/env python3

import re
import string

from itertools import count, chain
from functools import lru_cache

PARTITION_NAMES = tuple(string.digits + "_" + string.ascii_lowercase)
SPECIAL_PREFIXES = frozenset({"other", "punctuation"})
        
def pos_tagged(word):
    """Returns if a word either is or contains a POS tag."""

    # A part-of-speech (POS) tag can either be appended to the end of a word, as
    # in "eat_VERB", or can be a placeholder on its own, as in "_VERB_".
    pos_pattern = re.compile("_[A-Z.]+_?$")
    
    for potential_tag in pos_pattern.findall(word.decode()):
        if potential_tag.strip("_") in POS_TAGS:
            return False
    return True
