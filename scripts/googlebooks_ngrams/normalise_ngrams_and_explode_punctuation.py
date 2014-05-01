#!/usr/bin/env python3

descr = """
This script will first find the closest ASCII equivalent of each token in an
ngram and then discard all non-alphanumeric characters. The resulting ngrams
may become no longer unique and also shorter.
"""

import argparse
import os
import string

from collections import namedtuple

from unidecode import unidecode

from pysteg.googlebooks_ngrams.ngrams_analysis import bs_word_partition
from pysteg.googlebooks_ngrams.ngrams_analysis import gen_ngram_descriptions
from pysteg.googlebooks_ngrams.ngrams_analysis import ngram_filename

def word_prefix(word, n):
    """
    Returns the prefix of a word. The function is simple because it is to be
    used solely by this script - we can assume that the word consists only of
    alphanumeric characters or is a _START_/_END_ token.
    """

    if word == "_START_" or word == "_END_":
        return "other"
    if n == 1:
        return word[0]
    else:
        if len(word) == 1 or word[1] not in string.ascii_lowercase:
            return word[0] + "_"
        else:
            return word[:2]

def output_ngram(l, count, out)
    """
    Output a normalised ngram to an appropriate file. The input ngram includes
    empty tokens.
    """

    # Perform left and right merges
    for i in range(n):
        if type(l[i]) == Merge:
            merge = l[i]
            if merge.dir == -1:
                l[i-1] = l[i-1] + merge.word
            elif merge.dir == +1:
                l[i+1] = merge.word + l[i+1]

    # Remove empty tokens
    l = tuple(w for w in l if not (w == "" or type(w) == Merge))

    # Check the new length
    new_n = len(l)

    # See if an appropriate output file is already open
    new_prefix = word_prefix(l[0], new_n)
    if (new_n, new_prefix) not in out:
        out_filename = ngram_filename(new_n, new_prefix)
        out_path = os.path.join(args.output, out_filename)
        out[(new_n, new_prefix)] = open(out_path, "a")

    # Write the positive count to the output file
    out[(new_n, new_prefix)].write("\t".join(l + (count,)))

def process_file(descr):
    """
    Process a single file. Since ngrams will change size and partition, they
    will be appended to existing files containing ngram counts from other prefix
    files. As a result, changes introduces by partial processing of a file
    cannot be rolled back easily -- there is no progress tracking, the whole
    script needs to be restarted from scratch if interrupted midway.
    """

    n, in_prefix = descr
    n = int(n)

    in_filename = ngram_filename(n, in_prefix)
    in_path = os.path.join(args.input, in_filename)

    # Dictionary of all possible output files
    out = dict()

    # Check if a character is allowed through normalisation
    allowed_characters = frozenset(string.digits + string.ascii_lowercase)
    def allowed(c):
        return c in allowed_characters

    # Named tuple for merging tokens during normalisation:
    # dir is the direction merging, it is equal to -1 for left and +1 for right
    # word is the resulting word to be appended to its neighbour
    Merge = namedtuple('Merge', 'dir word')

    def normalise(word):
        """
        Normalise the word depending on whether it's a sentence marker, a
        special case to be merged or a normal word to be filtered from
        non-alphanumeric characters.
        """
        if word == "_START_" or word == "_END_":
            # Sentence delimiters stay unchanged
            return word
        if word.lower() == "'s":
            # Possessive <'s> or contracted <is> need to be left merged
            return Merge(-1, "s")
        else:
            # Otherwise convert Unicode to the best ASCII representation and
            # leave only alphanumeric characters
            return ''.join(filter(allowed, unidecode(word).lower()))

    with open(in_path, "r") as i:
        for line in i:
            l_original = line.split("\t")

            # Normalise individual tokens
            l = [normalise(w) for w in l_original[:-1]]

            # Discard ngrams with empty token on the edge - a lower order
            # ngram already handles these counts
            if l[0] == "" or l[-1] == "":
                continue

            # Discard ngrams with merge tokes pointing outwards - the edge
            # tokens are also effectively empty since they belong to a word
            # outside the ngram
            if ((type(l[0]) == Merge and l[0].dir == -1) or
                (type(l[-1]) == Merge and l[-1].dir == +1)):
                continue

            # Discard ngrams with merge tokens pointing towards an empty token -
            # these are undefined and cannot be handled elegantly
            for i in range(n):
                if type(l[i]) == Merge and l[i+merge.dir] == "":
                    continue

            # The next step would be to output the merged ngram counts, with
            # negative counts corresponding to lower order ngrams. I have
            # however realised that this is problematic. The next version of the
            # script will explore instead of joining ngrams.

if __name__ == '__main__':
    # Define and parse the script arguments
    parser = argparse.ArgumentParser(description=descr)
    parser.add_argument("ngrams",
        help="JSON file listing all ngram file descriptions")
    parser.add_argument("input",
        help="input directory with original ngram counts")
    parser.add_argument("output",
        help="output directory for counts of l ngram")
    args = parser.parse_args()

    ngram_descriptions = gen_ngram_descriptions(args.ngrams)

    process_file(("3", "th"))
