#!/usr/bin/env python3

descr = """
This script will translate all words in Google Books Ngrams files to integer
indices, sort them alphabetically and save in a binary file including the
cumulative frequencies.

Each line of the index file line has to consist of an integer and a
corresponding word, separated by a tab.
"""

import argparse
import json
import struct

from itertools import chain, count
from os import path

from numpy import zeros

from pysteg.googlebooks2 import PARTITION_NAMES, SPECIAL_PREFIXES
from pysteg.googlebooks_ngrams.ngrams_analysis import ngram_filename

def write_ngrams_table(n, prefixes):
    """Writes a cumulative frequencies ngrams table for a particular n."""
    
    # Prepare a schedule of reading ngrams from prefix files
    schedule = {part:set() for part in PARTITION_NAMES}
    for pref in prefixes:
        if pref in SPECIAL_PREFIXES:
            schedule["_"].add(pref)
        else:
            schedule[pref[0]].add(pref)
    
    # Create the file with all ngrams
    output_path = path.join(args.output, "{n}gram".format(**locals()))
    with open(output_path, "wb") as fo:
        # Prepare the line format specifier
        fmt = "<" + n * "i" + "q"
        dtp = [("w{}".format(i),"<i4") for i in range(n)] + [("f","<i4")]
    
        # Write the first line consisting of all columns equal to 0
        cf = 0
        fo.write(struct.pack(fmt, *n*(0,) + (cf,)))
        
        # Go over the partitions schedule
        for part in PARTITION_NAMES:
            # Count the maximum number of ngrams in the partition
            ngrams_maxn = sum(
                sum(1 for line
                    in open(path.join(args.input, ngram_filename(n,pref)), "r"))
                for pref in schedule[part]
            )
        
            # Create a numpy array that can contain all potential ngrams
            ngrams = zeros(ngrams_maxn, dtype=dtp)
            
            # Read one by one prefix files corresponding to the partition
            i = 0
            for pref in schedule[part]:
                # Simultaneously read ngrams from the prefix file and write
                # those which don't match to the error file 
                filename = ngram_filename(n,pref)
                input_path = path.join(args.input, filename)
                error_path = path.join(args.error, filename)
                with open(input_path, "r") as fi, open(error_path, "w") as fe:
                    for line in fi:
                        ngram = line[:-1].split("\t")
                        try:
                            # Translate all words into their indices
                            ixs = tuple(map(lambda x: w2i[x][0], ngram[:-1]))
                            # Assert that the partition is correct
                            assert(w2i[ngram[0]][1] == part)
                            # Add the ngram
                            ngrams[i] = ixs + (int(ngram[-1]),)
                            i+=1
                        # If the partition doesn't match or the word cannot be
                        # found in the index
                        except (AssertionError, KeyError):
                            fe.write(line)
                    print("Read ngrams from {input_path}".format(**locals()))
            ngrams_n = i
            
            # Sort and dump the partition
            ngrams = ngrams[:ngrams_n]
            ngrams.sort(order=["w{}".format(i) for i in range(n)])
            for i in range(ngrams_n):
                cf += ngrams[i]["f"]
                fo.write(struct.pack(fmt, *tuple(ngrams[i])[:-1] + (cf,)))
            print("Dumped indexed and cumulated ngrams partition {part} to "
                  "{output_path}".format(**locals()))

if __name__ == '__main__':
    # Define and parse arguments
    parser = argparse.ArgumentParser(
        description=descr,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("ngrams", help="JSON file listing all the ngram files")
    parser.add_argument("index", help="index file")
    parser.add_argument("input", help="input directory of ngram files")
    parser.add_argument("output", help="output directory of ngram files")
    parser.add_argument("error", help="output directory for discarded ngrams")
    args = parser.parse_args()

    # Read the index of words
    def read_index_line(line):
        """Given an index file line returns a (word, (ix, partition)) tuple."""
        line_split = line[:-1].split("\t")
        return (line_split[1], (int(line_split[0]), line_split[2]))
    with open(args.index, "r") as f:
        w2i = dict(map(read_index_line, f))
    
    # Load the ngram descriptions
    with open(args.ngrams, "r") as f:
         ngram_descriptions = json.load(f)
    
    # Create all bytes ngram tables
    for n, prefixes in ngram_descriptions.items():
        write_ngrams_table(int(n), prefixes)
