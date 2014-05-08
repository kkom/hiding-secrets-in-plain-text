#!/usr/bin/env python3

descr = """
This script translates chosen lines of a bindb table from indices to tokens and
outputs them to the terminal.

In the interactive window, type a single line number or a range of lines using
start:stop or start:step:stop notation. Type q to quit.
"""

import argparse
import re
import readline
import struct

from pysteg.common.log import print_status

from pysteg.googlebooks_ngrams import bindb

parser = argparse.ArgumentParser(description=descr)
parser.add_argument("n", type=int, help="order of the BinDB table")
parser.add_argument("bindb", help="BinDB file")
parser.add_argument("-t", "--translate", metavar="index",
    help="translate token indices to strings using an index file")
args = parser.parse_args()

if args.translate:
    print_status("Started loading index from", args.translate)
    with open(args.translate, "r") as f:
        index = bindb.BinDBIndex(f)
    print_status("Finished loading index")

# Patterns of the range definitions
single_line_pattern = re.compile("^\d+$")
range_pattern = re.compile("^(?P<start>\d+):((?P<step>\d+):)?(?P<stop>\d+)$")

# Function to represent a token based on its integer index
token_repr = index.i2s if args.translate else str

with open(args.bindb, "rb") as b:
    while True:
        ip = input('--> ')

        # Exit command
        if ip == "q":
            break

        # Try to match the range command, if cannot do it - continue
        single_line_m = single_line_pattern.match(ip)
        range_m = range_pattern.match(ip)
        if single_line_m:
            start = int(ip)
            step = 1
            stop = start + 1
        elif range_m:
            start = int(range_m.group('start'))
            stop = int(range_m.group('stop'))
            step = int(range_m.group('step')) if range_m.group('step') else 1
        else:
            continue

        # Print the actual lines
        try:
            for l in range(start,stop,step):
                bindb_line = bindb.read_line(b, args.n, l)
                ngram = "\t".join(map(token_repr, bindb_line[0]))
                count = bindb_line[1]
                print("{l}:\t{ngram}\t{count}".format(**locals()))

        except IOError as err:
            print("IOError: {err}".format(**locals()))
            print("Probably requested a line out of range.")
        except struct.error as err:
            print("struct.error: {err}".format(**locals()))
            print("Probably requested a line out of range.")
        except ValueError as err:
            print("ValueError: {err}".format(**locals()))
            print("Probably requested a line out of range.")
