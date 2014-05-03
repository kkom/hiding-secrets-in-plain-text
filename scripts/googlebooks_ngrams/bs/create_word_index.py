#!/usr/bin/env python3

descr = """
This script will create a words index from all Google Books 1grams.
"""

import argparse
import json
import os

from itertools import count, repeat

from pysteg.googlebooks_ngrams.ngrams_analysis import ngram_filename
from pysteg.googlebooks_ngrams.ngrams_analysis import BS_PARTITION_NAMES
from pysteg.googlebooks_ngrams.ngrams_analysis import BS_SPECIAL_PREFIXES

if __name__ == '__main__':
    # Define and parse the script arguments
    parser = argparse.ArgumentParser(description=descr)
    parser.add_argument("ngrams", help="JSON file listing all the ngram files")
    parser.add_argument("input", help="input directory for ngram files")
    parser.add_argument("output", help="output path for words index")
    args = parser.parse_args()

    # Read ngram descriptions file
    with open(args.ngrams, "r") as f:
        ngrams = json.load(f)

    # Create a dictionary whose keys are partitions and values are prefixes that
    # belong to the respective partitions. Most 1-gram prefixes, i.e. other than
    # BS_SPECIAL_PREFIXES, correspond 1:1 to partitions. There is also a special
    # partition "_" that words from BS_SPECIAL_PREFIXES belong to.
    part2pref = {p:(p,) for p in ngrams["1"] if p not in BS_SPECIAL_PREFIXES}
    part2pref["_"] = BS_SPECIAL_PREFIXES

    # Verify that the implicitly created partitions are correct
    assert(set(part2pref.keys()) == set(BS_PARTITION_NAMES))

    # Go over all partitions and read words from the corresponding prefix files
    gen_index = count(1)
    with open(args.output, "w") as fo:
        for part in BS_PARTITION_NAMES:
            # Initialise all words
            if part == "_":
                words = {"_START_", "_END_"}
            else:
                words = set()

            # Read words from respective prefix files
            for pref in part2pref[part]:
                path = os.path.join(args.input, ngram_filename(1, pref))
                if os.path.isfile(path):
                    with open(path, "r") as fi:
                        for line in fi:
                            words.add(line.split("\t")[0])
                    print("Read words from {path}".format(**locals()))

            # Dump words to the index file
            for w, i in zip(sorted(words), gen_index):
                fo.write("{i}\t{w}\t{part}\n".format(**locals()))
            print("Dumped {part} partition".format(**locals()))
