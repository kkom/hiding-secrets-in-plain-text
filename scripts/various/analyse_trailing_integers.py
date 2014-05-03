#!/usr/bin/env python3

descr = """
This script will analyse integers occurring at the end of each line of a text
file. It will output their sum and the largest and smallest integers.
"""

import argparse
import itertools
import math
import re

# Pattern of an integer occurring at the end of a line (preceded by whitespace)
pattern = re.compile("\s[0-9]+$")

def line_integer(line):
    """Get trailing integer from a line."""
    match = pattern.search(line)
    if match:
        return int(match.group())
    else:
        return float('nan')

def process_file(file):
    """Print the sum, minimum and maximum of trailing integers in a file."""

    total = 0
    min_i = float('inf')
    max_i = -float('inf')

    with open(file, "r") as f:
        for i in itertools.filterfalse(math.isnan, map(line_integer, f)):
            total = total + i
            min_i = min(min_i, i)
            max_i = max(max_i, i)

    print("{file}: sum {total}".format(**locals()))
    print("{file}: min {min_i}".format(**locals()))
    print("{file}: max {max_i}".format(**locals()))

# Define and parse the script arguments
parser = argparse.ArgumentParser(description=descr)
parser.add_argument("file", nargs='+')
args = parser.parse_args()

# Run the actual job
for file in args.file:
    process_file(file)
