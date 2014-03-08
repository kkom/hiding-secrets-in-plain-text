#!/usr/bin/env python3

descr = """
This script will create a words index from all Google Books 1grams.
"""

import argparse
import json

from itertools import count, repeat
from os.path import join

from pysteg.googlebooks2 import PARTITION_NAMES, SPECIAL_PREFIXES
from pysteg.googlebooks_ngrams.ngrams_analysis import ngram_filename

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
    
    # Create 1:many partitions to prefixes correspondence schedule
    schedule = {p:frozenset({p}) for p in ngrams["1"]
                if p not in SPECIAL_PREFIXES}
    schedule["_"] = SPECIAL_PREFIXES
    
    # Verify that the implicitly created partitions are correct
    assert(set(schedule.keys()) == set(PARTITION_NAMES))
    
    # Go over the schedule
    gen_index = count(1)
    with open(args.output, "w") as fo:
        for part in PARTITION_NAMES:
            # Initialise all words
            if part == "_":
                words = {"_START_", "_END_"}
            else:
                words = set()
            
            # Read words from respective prefix files
            for pref in schedule[part]:
                path = join(args.input, ngram_filename(1, pref))
                with open(path, "r") as fi:
                    for line in fi:
                        words.add(line[:-1].split("\t")[0])
                print("Read words from {path}".format(**locals()))
            
            # Dump words to the index file
            for w, i in zip(sorted(words), gen_index):
                fo.write("{i}\t{w}\t{part}\n".format(**locals()))
            print("Dumped {part} partition".format(**locals()))
