#!/usr/bin/env python3

descr = """
This script strips Project Gutenberg information text from the beginning and
end of an e-book. Normally used for preparing the text to build a histogram of
ngrams. 
"""

import argparse
import os
import re

# Define and use the parser
parser = argparse.ArgumentParser(
    description=descr,
    formatter_class=argparse.RawDescriptionHelpFormatter
)
parser.add_argument("-s", "--suffix", default=" (stripped)", required=False,
    help="suffix of output files, default: ' (stripped)'")
parser.add_argument("input", nargs='+', help="source text files")
args = parser.parse_args()

# Prepare the patterns
start = re.compile('^.*\*{3}start.*project.*gutenberg.*e.*book.*\*{3}.*$',
    flags=re.IGNORECASE)
end = re.compile('^.*\*{3}end.*project.*gutenberg.*e.*book.*\*{3}.*$',
    flags=re.IGNORECASE)

# Strip the start and end notices
for text in args.input:
    (name, ext) = os.path.splitext(text)

    with open(text, 'r') as f1:
        with open("{n}{s}{e}".format(n=name,s=args.suffix,e=ext), 'w') as f2:
            for r in f1:
                if start.search(r):
                    break
            
            for r in f1:
                if end.search(r):
                    break
            
                f2.write(r)
                
