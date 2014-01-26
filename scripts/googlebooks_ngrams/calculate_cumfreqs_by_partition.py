#!/usr/bin/env python3

descr = """
This script will calculate cumulative frequency ranges for each partition and
each n.
"""

import argparse
import json
import os

from pysteg.googlebooks import get_partition
from pysteg.googlebooks_ngrams.ngrams_analysis import ngram_filename
            
def calculate_cumfreq(path):
    """Sum the frequency of all ngrams in a file."""
    s = 0
    with open(path, "r") as f:
        for line in f:
            s += int(line.split()[-1])
    return s

if __name__ == '__main__':
    # Define and parse the script arguments
    parser = argparse.ArgumentParser(description=descr)
    parser.add_argument("ngrams", help="JSON file listing all the ngram files")
    parser.add_argument("input", help="input directory of ngram files")
    parser.add_argument("output",
        help="location to save the JSON file with cumulative frequency ranges")
    args = parser.parse_args()

    # Read the ngram descriptions
    with open(args.ngrams, 'r') as f:
        ngrams = json.load(f)
    
    # Partitions are the 1gram prefixes ordered alphabetically
    partitions = sorted(ngrams["1"])
    partitions_set = frozenset(partitions)
    
    # Dictionary holding cumulative frequency ranges in each partition
    cumfreq_ranges = {}
    
    for n in sorted(ngrams.keys()):
        # Calculate total frequencies in each partition
        cumfreqs = {}
        for prefix in ngrams[n]:
            partition = get_partition(prefix, partitions_set)
            path = os.path.join(args.input, ngram_filename(n,prefix))
            cumfreqs[partition] = (cumfreqs.get(partition, 0)
                + calculate_cumfreq(path))
            print("Counted cumulative frequency for FILE {path}".format(
                **locals()))
        
        # Calculate cumulative frequency ranges in each partition
        cumfreq_ranges[n] = {}
        cumfreq = 0
        for partition in partitions:
            cumfreq_ranges[n][partition] = (cumfreq,
                                            cumfreq + cumfreqs[partition])
            cumfreq += cumfreqs[partition]
    
    with open(args.output, "w") as f:
        json.dump(cumfreq_ranges, f)
    print("Saved cumulative frequency ranges to JSON FILE {args.output}".format(
        **locals()))
    