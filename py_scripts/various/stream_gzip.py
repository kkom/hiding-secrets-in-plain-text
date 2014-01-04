#!/usr/bin/env python3

descr = """
This script streams a gzip-compressed file from a network location. Lines are
numbered from 0.
"""

import argparse
import gzip
import itertools
import urllib.request

from libsteg.common.timing import timer

# Define and parse the script arguments
parser = argparse.ArgumentParser(
    description=descr,
    formatter_class=argparse.RawDescriptionHelpFormatter
)

parser.add_argument("URL", help="web address of the file")

parser.add_argument("--start", type=int, default=0,
    help="skip lines before this one")
parser.add_argument("--stop", type=int, default=None,
    help="only show lines before this one")
parser.add_argument("--step", type=int, default=1,
    help="step size")

args = parser.parse_args()

# Define iterators used in the script
def iter_remote_gzip(url):
    with urllib.request.urlopen(url) as f:
        with gzip.GzipFile(fileobj=f) as g:
            for l in g:
                yield l

line_numbers = map(lambda x: args.start + args.step*x, itertools.count())
lines = itertools.islice(
    iter_remote_gzip(args.URL), args.start, args.stop, args.step
)

# Run and time the iterators
with timer(method="time") as t:
    for (i,l) in zip(line_numbers, lines):
        print("{}: {}".format(i, l.decode()), end="")
        
print('The request took {:.2f} s of real time.'.format(t.interval))
