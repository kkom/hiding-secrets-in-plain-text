#!/usr/bin/env python3

descr = """
This script will translate all tokens from Google Books Ngrams files to integer
indices, sort them alphabetically, deduplicate ngrams keeping total counts and
save them in bindb format.

The bindb format specifies separately for each ngram order n a single binary
file storing ngram counts. Ngrams are output sorted by their indices. For each
ngram the following bytes are saved in little endian order:

n * 4 byte integers with indices of tokens
    8 byte integer with ngram count
"""

import argparse
import json
import os
import struct

from itertools import count

from numpy import zeros

from pysteg.common.log import print_status

from pysteg.googlebooks_ngrams import bindb

from pysteg.googlebooks_ngrams.ngrams_analysis import ngram_filename
from pysteg.googlebooks_ngrams.ngrams_analysis import BS_PARTITION_NAMES
from pysteg.googlebooks_ngrams.ngrams_analysis import BS_SPECIAL_PREFIXES

def write_ngrams_table(n, prefixes):
    """Writes ngrams counts table for a particular n."""

    def pref_path(pref):
        """Give path to a prefix file."""
        return os.path.join(args.input, ngram_filename(n,pref))

    # Prepare a part2pref dictionary of prefixes corresponding to partitions
    part2pref = {part:set() for part in BS_PARTITION_NAMES}
    for pref in prefixes:
        # Determine which prefix files actually exist. This introduces a race
        # condition, however the assumption is that database will not be
        # modified while this script is running.
        if os.path.exists(pref_path(pref)):
            if pref in BS_SPECIAL_PREFIXES:
                part2pref["_"].add(pref)
            else:
                part2pref[pref[0]].add(pref)

    # Format specifier for a line of the bindb file
    fmt = bindb.fmt(n)

    # Format specifier for the numpy matrix used for sorting the ngrams
    dtp = (
        # n * little-endian 4 byte integers with token indices
        [("w{}".format(i),"<i4") for i in range(n)] +
        # little-endian 8 byte integer with ngram count
        [("f","<i8")]
    )

    # Create the bindb file
    output_path = os.path.join(args.output, "{n}gram".format(**locals()))
    with open(output_path, "wb") as fo:
        # Go over the prefix files for each possible partitions
        for part in BS_PARTITION_NAMES:
            # Sort the set of prefixes which will contribute to this partition
            # to take advantage of partial sorting (ngrams belonging to the same
            # prefix will still be adjacent in the sorted partition)
            prefs = sorted(part2pref[part])

            # Calculate the maximum number of ngrams in the partition by
            # counting total number of lines in each prefix file
            ngrams_maxn = sum(
                sum(1 for line in open(pref_path(pref), "r")) for pref in prefs
            )

            # Create a numpy array that can contain all potential ngrams
            ngrams = zeros(ngrams_maxn, dtype=dtp)

            # Read one by one prefix files corresponding to the partition
            i = 0
            for pref in prefs:
                # Simultaneously read ngrams from the prefix file and write
                # those which don't match to the error file
                filename = ngram_filename(n, pref)
                input_path = os.path.join(args.input, filename)
                error_path = os.path.join(args.error, filename)
                with open(input_path, "r") as fi, open(error_path, "w") as fe:
                    for line in fi:
                        ngram = line[:-1].split("\t")
                        try:
                            # Translate all tokens to their indices
                            ixs = tuple(map(index.s2i, ngram[:-1]))
                            # Assert that the partition is correct
                            assert(index.s2p(ngram[0]) == part)
                            # Add the ngram
                            ngrams[i] = ixs + (int(ngram[-1]),)
                            i+=1
                        # If the partition doesn't match or the token cannot be
                        # found in the index
                        except (AssertionError, KeyError):
                            fe.write(line)
                print_status("Read and indexed ngrams from", input_path)
            ngrams_n = i

            # Sort the partition
            ngrams = ngrams[:ngrams_n]
            ngrams.sort(order=["w{}".format(i) for i in range(n)])
            print_status(ngrams_n, "ngrams sorted")

            # Write lines to the binary counts file
            out_count = 0
            current_ngram = tuple()
            current_f = 0
            for i in range(ngrams_n):
                ngram_i = tuple(ngrams[i])[:-1]

                # Compare this ngram to the currently deduplicated ngram
                if ngram_i == current_ngram:
                    current_f += ngrams[i]["f"]
                else:
                    if i != 0:
                        fo.write(struct.pack(fmt, *current_ngram+(current_f,)))
                        out_count += 1
                    current_ngram = ngram_i
                    current_f = ngrams[i]["f"]

                # Write a line in the last loop iteration
                if i == ngrams_n-1:
                    fo.write(struct.pack(fmt, *current_ngram+(current_f,)))
                    out_count += 1

            print_status(out_count, "ngrams integrated and saved to",
                         output_path)

if __name__ == '__main__':
    # Define and parse arguments
    parser = argparse.ArgumentParser(
        description=descr,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("ngrams", help="JSON file listing all the ngram files")
    parser.add_argument("index", help="index file")
    parser.add_argument("input", help="input directory of ngram files")
    parser.add_argument("output", help="output directory of bindb files")
    parser.add_argument("error", help="output directory for discarded ngrams")
    args = parser.parse_args()

    # Read the index of tokens
    print_status("Started loading index from", args.index)
    with open(args.index, "r") as f:
        index = bindb.BinDBIndex(f)
    print_status("Finished loading index")

    # Load the ngram files descriptions
    with open(args.ngrams, "r") as f:
         ngram_descriptions = json.load(f)

    # Create all bindb tables
    for n, prefixes in ngram_descriptions.items():
        write_ngrams_table(int(n), prefixes)
