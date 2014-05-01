#!/usr/bin/env python3

descr = """
This script will first find the closest ASCII equivalent of each token. It will
then remove all characters which are not digits, letters or standard
punctuation. All tokens will be exploded by punctuation marks -- a valid token
will consist of any number of alphanumeric characters or a single punctuation
mark. Induced ngrams of the order not exceeding the original order of the input
database will be then output.
"""

import argparse
import datetime
import itertools
import os
import string

from unidecode import unidecode

from pysteg.googlebooks_ngrams.ngrams_analysis import gen_ngram_descriptions
from pysteg.googlebooks_ngrams.ngrams_analysis import ngram_filename

__alphabetic_charset = frozenset(string.ascii_lowercase)
__numeric_charset = frozenset(string.digits)
__punctuation_charset = frozenset(string.punctuation)
__alphanumeric_charset = __alphabetic_charset.union(__numeric_charset)
__normalised_charset = __alphanumeric_charset.union(__punctuation_charset)

def allowed(c):
    """Check if a character is allowed through normalisation."""
    return c in __normalised_charset

def close_output_files(out):
    """Close the output files and remove them from the dictionary."""
    for i in tuple(out.keys()):
        out[i].close()
        del out[i]

def explode(token):
    """Explode the token by punctuation characters."""

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

def normalise(token):
    """
    Normalise the token. A sentence marker will stay unchanged, a token without
    punctuation will be normalised to its alphanumerical representation
    (possibly empty in case it only consists of special characters), and a token
    with punctuation will be split by punctuation.
    """
    if token == "_START_" or token == "_END_":
        # Sentence delimiters stay unchanged
        return (token,)
    else:
        # Convert token from Unicode to the best ASCII representation and
        # leave only the allowed characters
        token = ''.join(filter(allowed, unidecode(token).lower()))

        # Explode the token by punctuation characters
        return explode(token)

def output_ngram(l, count, out):
    """
    Output a normalised ngram to an appropriate file. The input ngram includes
    empty tokens.
    """
    n = len(l)

    # See if an appropriate output file is already open
    prefix = token_prefix(l[0], n)
    if (n, prefix) not in out:
        # Close all files if too many are open. The elegant way would be to
        # maintain the files in the order of last access and close only the one
        # that was accessed longest time ago, but this hack works for now and
        # efficiency is not key in this script.
        if len(out) > 1000:
            close_output_files(out)

        filename = ngram_filename(n, prefix)
        path = os.path.join(args.output, filename)
        out[(n, prefix)] = open(path, "a")

    # Write the positive count to the output file
    out[(n, prefix)].write("\t".join(l + (count,)))

def print_status(message, filename):
    """Output status of processing a file to the terminal."""

    time = datetime.datetime.now()
    print("{time} {message} {filename}".format(**locals()))

def token_prefix(token, n):
    """
    Return the prefix of a token. After normalisation a token can consist of
    lowercase letters, digits and punctuation marks, so it suffices to check the
    first character (in case of 1-grams) or the first two characters (in case of
    2-grams).
    """

    if token in ("_START_", "_END_") or token[0] not in __alphanumeric_charset:
        return "other"
    elif n == 1 or token[0] in __numeric_charset:
        return token[0]
    elif len(token) == 1 or token[1] not in __alphanumeric_charset:
        return token[0] + "_"
    else:
        return token[:2]

def process_file(descr, max_n):
    """
    Process a single file. Since ngrams will change size and partition, they
    will be appended to existing files containing ngram counts from other prefix
    files. As a result, changes introduces by partial processing of a file
    cannot be rolled back easily -- there is no progress tracking, the whole
    script needs to be restarted from scratch if interrupted midway.
    """

    n, prefix = descr
    n = int(n)

    filename = ngram_filename(n, prefix)
    path = os.path.join(args.input, filename)

    print_status("Processing", filename)

    # Dictionary of all possible output files
    out = dict()

    with open(path, "r") as i:
        for line in i:
            l_original = line.split("\t")

            # Normalise and explode original tokens
            l = tuple(normalise(w) for w in l_original[:-1])

            # Count the exploded size of each original token
            s = tuple(len(token) for token in l)

            # Discard ngrams with empty original edge tokens - a lower order
            # ngram already handles these counts
            if s[0] == 0 or s[-1] == 0:
                continue

            # There are at least two original tokens, so both edge tokens exist
            if n >= 2:
                # Count the total exploded size of middle original tokens, these
                # have to be included in the output
                middle_s = sum(s[1:-1])

                # Count the maximum number of normalised tokens that can come
                # from the original edge tokens
                max_edge_s = max_n - middle_s

                # There are too many exploded middle tokens -- the normalised
                # ngram including at least one normalised token from each
                # original edge token would be beyond the order of the model
                if max_edge_s < 2:
                    continue

                # Limit the maximum number of normalised edge token by the
                # combined sizes of exploded original edge tokens
                max_ts = min(max_edge_s, s[0]+s[1])

                # Flatten the original middle tokens
                l_middle = tuple(itertools.chain.from_iterable(l[1:-1]))

                # Consider every combination of normalised edge tokens -- they
                # need to be adjacent to the middle tokens
                for ls in range(1,max_ts):
                    for rs in range(1,max_ts-ls+1):
                        output_ngram(l[0][-ls:] + l_middle + l[-1][:rs],
                                     l_original[-1], out)

            # There is only one original token
            else:
                # Maximum number of normalised tokens coming from the original
                # token
                max_s = min(max_n,s[0])

                for start in range(0, s[0]):
                    for stop in range(start+1, min(start+max_s,s[0])+1):
                        output_ngram(l[0][start:stop], l_original[-1], out)

    close_output_files(out)

    print_status("Finished", filename)

if __name__ == '__main__':
    # Define and parse the script arguments
    parser = argparse.ArgumentParser(description=descr)
    parser.add_argument("ngrams",
        help="JSON file listing all ngram file descriptions")
    parser.add_argument("n_max", type=int,
        help="maximum order of output ngrams")
    parser.add_argument("input",
        help="input directory with original ngram counts")
    parser.add_argument("output",
        help="output directory for counts of l ngram")
    args = parser.parse_args()

    ngram_descriptions = gen_ngram_descriptions(args.ngrams)

    print("Remember that {args.output} needs to be cleared before running the "
          "script.".format(**locals()))

    for ngram in ngram_descriptions:
        process_file(ngram, args.n_max)
