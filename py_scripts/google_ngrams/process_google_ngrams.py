#!/usr/bin/env python3

descr = """
This script processes a Google Books Ngrams file from a web location and saves
the processed ngram counts locally.
"""

import argparse
import itertools

from pysteg.common.streaming import iter_remote_gzip 
from pysteg.google_ngrams.extract_ngram_counts import extract_ngram_counts

# Define and parse the script arguments
parser = argparse.ArgumentParser(
    description=descr,
    formatter_class=argparse.RawDescriptionHelpFormatter
)

parser.add_argument("URL", help="web address of the file")
parser.add_argument("n", type=int, help="size of the ngrams")
parser.add_argument("output", help="local address of the output file")
parser.add_argument("-t", type=int, default=None,
    help="process only T lines from the source file")
    
args = parser.parse_args()

# Define iterators used in the script
source_ngrams = itertools.islice(iter_remote_gzip(args.URL), 0, args.t)
processed_ngrams = extract_ngram_counts(source_ngrams, args.n)

for ngram in processed_ngrams:
    print(ngram)
