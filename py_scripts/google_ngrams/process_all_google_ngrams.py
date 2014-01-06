#!/usr/bin/env python3

descr = """
This script will process all Google Books Ngrams files listed in a JSON file and
save them locally.
"""

epilog = """
Note that the hours_off parameter will not stop the script from running an
already dispatched job.
"""

import argparse
import datetime
import io
import json
import multiprocessing
import os
import time
import urllib.parse

from pysteg.common.itertools import consume
from pysteg.common.streaming import iter_remote_gzip
from pysteg.common.streaming import ngrams_iter2file

from pysteg.google_ngrams.extract_ngram_counts import extract_ngram_counts

# Define and parse the script arguments
parser = argparse.ArgumentParser(description=descr, epilog=epilog)

parser.add_argument("ngrams", help="JSON file listing all the ngram files")
parser.add_argument("output", help="output directory for processed files")
parser.add_argument("--processes", metavar="P", type=int, default=1,
    help="set the number of parallel worker processes (default 1)")
parser.add_argument("--hours_off", nargs=2, metavar=("START","END"),
    help=("stop the script from dispatching jobs between two hours (HH:MM "
          "format)"))
parser.add_argument("--days_on", metavar="DAY", type=int, nargs='+',
    help=("force the script to run on the specified days regardless of the "
          "hours_off parameter (days are numbered starting from Monday as 1)"))
    
args = parser.parse_args()

def allowed_to_dispatch():
    """Check if the script is allowed to dispatch another job to the pool."""
    
    if args.days_on:
        if datetime.date.today().isoweekday() in args.days_on:
            return True
        
    if args.hours_off:
        start = datetime.time(*map(int, args.hours_off[0].split(":")))
        end = datetime.time(*map(int, args.hours_off[1].split(":")))
        
        now = datetime.datetime.now()
        time_now = datetime.time(now.hour, now.minute)
    
        if time_now > start and time_now < end:
            return False
        else:
            return True

def process_file(descr):
    """Process a single file."""
    
    while not allowed_to_dispatch():
        time.sleep(300)
    
    filename_template = "googlebooks-eng-us-all-{n}gram-20120701-{prefix}"
    local_root = args.output
    remote_root = "http://storage.googleapis.com/books/ngrams/books/"
    
    n, prefix = descr
    
    filename = filename_template.format(n=n, prefix=prefix)
    local_path = os.path.join(local_root, filename)
    remote_path = urllib.parse.urljoin(remote_root, filename + ".gz")
    
    if os.path.isfile(local_path + "_DONE"):
        print("{t} Skipped {f}".format(t=datetime.datetime.now(),f=filename))
    else:
        print("{t} Processing {f}".format(t=datetime.datetime.now(),f=filename))
        
        # Generate iterators over ngrams
        source_ngrams = iter_remote_gzip(remote_path)
        processed_ngrams = extract_ngram_counts(source_ngrams, int(n))
        
        with io.open(local_path, 'wb') as f:
            ngrams_iter2file(processed_ngrams, f)
            
        open(local_path + "_DONE", 'w').close()
        
        print("{t} Finished {f}".format(t=datetime.datetime.now(),f=filename))

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
    