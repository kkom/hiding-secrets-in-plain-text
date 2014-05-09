#!/usr/bin/env python3

descr = """
This script will convert text to a sequence of tokens.
"""

import argparse
import readline

from pysteg.common.log import print_status

from pysteg.googlebooks import bindb

from pysteg.googlebooks.ngrams_analysis import normalise_and_explode_tokens
from pysteg.googlebooks.ngrams_analysis import text2token_strings

# Define and parse the script arguments
parser = argparse.ArgumentParser(description=descr)
parser.add_argument("-i", "--index",
    help="represent tokens using their indices")
parser.add_argument("-n", "--normalise", action="store_true",
    help="normalise and explode tokens")
args = parser.parse_args()

# Load the index
if args.index:
    print_status("Started loading index from", args.index)
    with open(args.index, "r") as f:
        index = bindb.BinDBIndex(f)
    print_status("Finished loading index")

while True:
    try:
        text = input('--> ')
    except KeyboardInterrupt:
        print()
        break

    token_strings = text2token_strings(text)

    if args.normalise:
        token_strings = normalise_and_explode_tokens(token_strings)

    print(" ".join(token_strings))

    if args.index:
        token_indices = tuple(map(index.s2i, token_strings))
        print(token_indices)
