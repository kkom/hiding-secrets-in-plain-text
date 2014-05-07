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
import multiprocessing
import os
import time
import urllib.parse

from pysteg.common.files import open_file_to_process, FileAlreadyProcessed

from pysteg.common.streaming import iter_remote_gzip
from pysteg.common.streaming import ngrams_iter2file

from pysteg.googlebooks_ngrams.ngrams_analysis import gen_ngram_descriptions
from pysteg.googlebooks_ngrams.ngrams_analysis import integrate_pure_ngram_counts
from pysteg.googlebooks_ngrams.ngrams_analysis import ngram_filename

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

    return True

def process_file(descr):
    """Process a single file."""

    while not allowed_to_dispatch():
        time.sleep(300)

    n, prefix = descr

    local_root = args.output
    remote_root = "http://storage.googleapis.com/books/ngrams/books/"

    filename = ngram_filename(n, prefix)
    local_path = os.path.join(local_root, filename)
    remote_path = urllib.parse.urljoin(remote_root, filename + ".gz")

    def print_status(message, filename):
        time = datetime.datetime.now()
        print("{time} {message} {filename}".format(**locals()))

    with open_file_to_process(local_path, "wb") as f:
        if f == False:
            print_status("Skipped", filename)
            raise FileAlreadyProcessed()

        print_status("Processing", filename)

        # Generate iterators over ngrams
        source_ngrams = iter_remote_gzip(remote_path)
        processed_ngrams = integrate_pure_ngram_counts(source_ngrams, n)

        # Save the integrated ngram counts to a file
        ngrams_iter2file(processed_ngrams, f)

        print_status("Finished", filename)

if __name__ == '__main__':
    # Define and parse the script arguments
    parser = argparse.ArgumentParser(description=descr, epilog=epilog)
    parser.add_argument("ngrams",
        help="JSON file listing all the ngram file descriptions")
    parser.add_argument("output", help="output directory for processed files")
    parser.add_argument("--processes", metavar="P", type=int, default=1,
        help="set the number of parallel worker processes (default 1)")
    parser.add_argument("--hours_off", nargs=2, metavar=("START","END"),
        help=("stop the script from dispatching jobs between two hours (HH:MM "
              "format)"))
    parser.add_argument("--days_on", metavar="DAY", type=int, nargs='+',
        help=("force the script to run on the specified days regardless of the "
              "hours_off parameter (days are numbered starting from Monday as "
              "1)"))
    args = parser.parse_args()

    ngram_descriptions = gen_ngram_descriptions(args.ngrams)

    if args.processes == 1:
        for ngram in ngram_descriptions:
            process_file(ngram)
    else:
        p = multiprocessing.Pool(processes=args.processes)
        p.map(process_file, ngram_descriptions, 1)
        p.close()
        p.join()
