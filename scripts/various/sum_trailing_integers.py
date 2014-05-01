#!/usr/bin/env python3

descr = """
This script will sum integers occurring at the end of each line of a text file.
"""

import argparse
import re

# Define and parse the script arguments
parser = argparse.ArgumentParser(description=descr)
parser.add_argument("file")
args = parser.parse_args()

# Pattern of an integer occurring at the end of a line (preceded by whitespace)
pattern = re.compile("\s[0-9]+$")

def line_integer(line):
    """Get trailing integer from a line."""
    match = pattern.search(line)
    if match:
        return int(match.group())
    else:
        return 0

with open(args.file, "r") as f:
    total = sum(map(line_integer, f))

print(total)
