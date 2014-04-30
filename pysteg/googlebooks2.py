import string

PARTITION_NAMES = tuple(string.digits + "_" + string.ascii_lowercase)
SPECIAL_PREFIXES = frozenset({"other", "punctuation"})
