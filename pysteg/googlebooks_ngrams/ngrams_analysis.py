import json
import re
import string

from unidecode import unidecode

__alphabetic_charset = frozenset(string.ascii_lowercase)
__numeric_charset = frozenset(string.digits)
__punctuation_charset = frozenset(string.punctuation)
__alphanumeric_charset = __alphabetic_charset.union(__numeric_charset)
__nonalphabetic_charset = __numeric_charset.union(__punctuation_charset)
__normalised_charset = __alphanumeric_charset.union(__punctuation_charset)

BS_PARTITION_NAMES = tuple(string.digits + "_" + string.ascii_lowercase)
BS_SPECIAL_PREFIXES = frozenset({"other", "punctuation"})

def integrate_pure_ngram_counts(source_ngrams, n):
    """
    Given an iterator over the lines of a Google Books Ngram corpus file return
    an iterator over the counts of particular ngrams. The counts are integrated
    over all years and ngrams containing syntactic annotations are discarded.

    The function assumes that all lines which have the same ngrams and differ
    only by the year of publication will be adjacent in the file. This
    assumption is needed to generate correct output and is critical to the
    function's performance.
    """

    # A part-of-speech (POS) tag can either be appended to the end of a word, as
    # in "eat_VERB", or can be a placeholder on its own, as in "_VERB_".
    pos_pattern = re.compile("_[A-Z.]+_?$")
    pos_tags = frozenset({"NOUN", "VERB", "ADJ", "ADV", "PRON", "DET", "ADP",
        "NUM", "CONJ", "PRT", ".", "X"})

    def pos_tagged(word):
        """
        Return if a word (represented by a Bytes array) either is or contains a
        POS tag.
        """
        for potential_tag in pos_pattern.findall(word.decode()):
            if potential_tag.strip("_") in pos_tags:
                return True
        return False

    def valid_ngram(ngram):
        """Check if the current ngram is valid, i.e. POS tag-free."""
        for word in ngram:
            if pos_tagged(word):
                return False
        return True

    current_ngram = []
    current_ngram_count = 0
    current_ngram_valid = False

    for line in source_ngrams:
        data = line.split()

        if data[0:n] == current_ngram:
            current_ngram_count += int(data[n+1])
        else:
            if current_ngram_valid:
                yield (tuple(current_ngram), current_ngram_count)

            current_ngram = data[0:n]
            current_ngram_count = int(data[n+1])
            current_ngram_valid = valid_ngram(current_ngram)

def gen_ngram_descriptions(filename):
    """Yields ngram descriptions from a file."""

    with open(filename, 'r') as f:
        ngrams = json.load(f)

    for n in sorted(ngrams.keys()):
        for prefix in ngrams[n]:
            yield (n, prefix)

def ngram_filename(n, prefix):
    """Return a standard ngram filename from its order and prefix."""

    return "googlebooks-eng-us-all-{n}gram-20120701-{prefix}".format(**locals())

def normalise_and_explode(token):
    """Normalise and then explode the token by punctuation characters."""

    def allowed(c):
        """Check if a character is allowed through normalisation."""
        return c in __normalised_charset

    # Convert token from Unicode to the best ASCII representation and
    # leave only the allowed characters
    token = ''.join(filter(allowed, unidecode(token).lower()))

    # If no characters are left, return the token as an empty tuple
    if len(token) == 0:
        return tuple()

    exploded = []

    interval_start = 0
    current_punctuation = False
    previous_punctuation = False

    for i in range(len(token)):
        current_punctuation = token[i] in __punctuation_charset

        if i != 0 and current_punctuation or previous_punctuation:
            exploded.append(token[interval_start:i])
            interval_start = i

        previous_punctuation = current_punctuation

    exploded.append(token[interval_start:])

    return tuple(exploded)

def token_bs_partition(token, n):
    """
    Return the partition of a token. After normalisation a token can consist of
    lowercase letters, digits and punctuation marks, so it suffices to check the
    first two characters.
    """

    if token in ("_START_", "_END_") or token[0] in __punctuation_charset:
        # Special symbols - sentence markers and punctuation
        return "_"
    elif token[0] in __numeric_charset:
        # Numeric partitions are single character
        return token[0]
    elif len(token) == 1 or token[1] in __nonalphabetic_charset:
        # Single character alphabetic partitions or those where the second
        # character is not a letter
        return token[0] + "_"
    else:
        # Standard two-character letter partitions
        return token[:2]
