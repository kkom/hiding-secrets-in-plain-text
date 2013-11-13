descr = """
This script strips Project Gutenberg text from the beginning and end of an
e-book. Normally used for preparing the text to build a histogram of n-grams. 
"""

import argparse
import os

# Define and use the parser
parser = argparse.ArgumentParser(
    description=descr,
    formatter_class=argparse.RawDescriptionHelpFormatter
)
parser.add_argument("-s", "--suffix", default=" (stripped)", required=False,
    help="suffix of output files, default: ' (stripped)'")
parser.add_argument("input", nargs='+', help="source text files")
args = parser.parse_args()

# Strip the start and end notices
for text in args.input:
    (name, ext) = os.path.splitext(text)

    with open(text, 'r') as f1:
        with open("{n}{s}{e}".format(n=name,s=args.suffix,e=ext), 'w') as f2:
            for r in f1:
                if r[:41] == "*** START OF THIS PROJECT GUTENBERG EBOOK":
                    break
            
            for r in f1:
                if r[:39] == "*** END OF THIS PROJECT GUTENBERG EBOOK":
                    break
            
                f2.write(r)
                