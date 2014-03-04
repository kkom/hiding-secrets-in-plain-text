#!/usr/bin/env python3

descr = """
This script will create a words index from all Google Books 1grams.
"""

import argparse

from itertools import count, repeat

from pysteg.googlebooks2 import create_partition_names, get_partition
from pysteg.googlebooks_ngrams.ngrams_analysis import gen_ngram_descriptions
from pysteg.googlebooks_ngrams.ngrams_analysis import ngram_filename

if __name__ == '__main__':
    # Define and parse the script arguments
    parser = argparse.ArgumentParser(description=descr)
    parser.add_argument("ngrams", help="JSON file listing all the ngram files")
    parser.add_argument("input", help="input directory for ngram files")
    parser.add_argument("output", help="output path for words index")
    args = parser.parse_args()

    # Prepare index partitions 
    index_partitions = {p:set() for p in create_partition_names()}
    
    # Put words into partitions
    ngram_descriptions = gen_ngram_descriptions(args.ngrams)
    for ngram_description in filter(lambda x: x[0] == "1", ngram_descriptions):
        path = args.input + ngram_filename(*ngram_description)
        with open(path, "r") as f:
            for l in f:
                line = l.split("\t")
                index_partitions[get_partition(line[0])].add(line[0])
        print("Processed {path}".format(**locals()))
    
    # Assign indices to words and save them
    gen_index = count(1)
    with open(args.output, "w") as f:
        for p in create_partition_names():
            for i, w in zip(gen_index, sorted(index_partitions[p])):
                f.write("{i}\t{w}\n".format(**locals()))
            print("Dumped {p} partition".format(**locals()))
