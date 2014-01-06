#!/usr/bin/env python3

descr = """
This script processes all Google Books Ngrams files listed in a JSON file and
saves them locally.
"""

import argparse
import io
import json
import multiprocessing
import os
import time
import urllib.parse

from datetime import datetime

from pysteg.common.itertools import consume
from pysteg.common.streaming import iter_remote_gzip
from pysteg.common.streaming import ngrams_iter2file

from pysteg.google_ngrams.extract_ngram_counts import extract_ngram_counts

# Define and parse the script arguments
parser = argparse.ArgumentParser(
    description=descr,
    formatter_class=argparse.RawDescriptionHelpFormatter
)

parser.add_argument("ngrams", help="JSON file listing all the ngram files")
parser.add_argument("output", help="output directory for processed files")
parser.add_argument("--processes", metavar="P", type=int, default=1,
    help="set the number of parallel worker processes (default 1)")
# parser.add_argument("--off", nargs=2, metavar=("START","END"),
#     help="disables the script between the two hours (HH:MM format)")
    
args = parser.parse_args()

def process_file(descr):
    """Process a single file."""
    
    filename_template = "googlebooks-eng-us-all-{n}gram-20120701-{prefix}"
    local_root = args.output
    remote_root = "http://storage.googleapis.com/books/ngrams/books/"
    
    n, prefix = descr
    
    filename = filename_template.format(n=n, prefix=prefix)
    local_path = os.path.join(local_root, filename)
    remote_path = urllib.parse.urljoin(remote_root, filename + ".gz")
    
    if os.path.isfile(local_path + "_DONE"):
        print("{t} Skipped {f}.".format(t=datetime.now(), f=filename))
    else:
        print("{t} Processing {f}...".format(t=datetime.now(), f=filename))
        
        # Generate iterators over ngrams
        source_ngrams = iter_remote_gzip(remote_path)
        processed_ngrams = extract_ngram_counts(source_ngrams, int(n))
        
        with io.open(local_path, 'wb') as f:
            ngrams_iter2file(processed_ngrams, f)
            
        open(local_path + "_DONE", 'w').close()
        
        print("{t} Finished {f}.".format(t=datetime.now(), f=filename))

def yield_ngram_descriptions(filename):
    """Yield ngram descriptions from a file."""
    
    with open(filename, 'r') as f:
        ngrams = json.load(f)
        
    for n in sorted(ngrams.keys()):
        for prefix in ngrams[n]:
            yield (n, prefix)

if __name__ == '__main__':
    p = multiprocessing.Pool(processes=args.processes)
    
    ngrams = yield_ngram_descriptions(args.ngrams)
    p.map(process_file, ngrams, 1)
    
    p.close()
    p.join()
    