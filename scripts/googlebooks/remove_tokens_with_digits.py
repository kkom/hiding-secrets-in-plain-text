#!/usr/bin/env python3

descr = """
This script will remove from raw string ngram counts all tokens with digits.
"""

import argparse
import itertools
import os
import string

from pysteg.common.files import open_file_to_process, FileAlreadyProcessed
from pysteg.common.log import print_status
from pysteg.googlebooks.ngrams_analysis import gen_ngram_descriptions
from pysteg.googlebooks.ngrams_analysis import ngram_filename
from pysteg.googlebooks.ngrams_analysis import numeric_token

def contains_digits(line):
    """Return whether any token in a line contains any digits."""
    return any(map(numeric_token, line.split("\t")[:-1]))

def process_file(n, prefix):
    """Process a single file."""

    filename = ngram_filename(n, prefix)
    output_path = os.path.join(args.output, filename)

    with open_file_to_process(output_path, "w") as o:
        if o == False:
            print_status("Skipped", filename)
            raise FileAlreadyProcessed()

        print_status("Processing", filename)

        if numeric_token(prefix):
            return

        input_path = os.path.join(args.input, filename)
        with open(input_path, "r") as i:
            for line in itertools.filterfalse(contains_digits, i):
                o.write(line)

# Define and parse the script arguments
parser = argparse.ArgumentParser(description=descr)
parser.add_argument("ngrams",
    help="JSON file listing all ngram file descriptions")
parser.add_argument("input",
    help="input directory with original ngram counts (text format)")
parser.add_argument("output",
    help="output directory for ngram counts without digits (text format)")
args = parser.parse_args()

ngram_descriptions = gen_ngram_descriptions(args.ngrams)

for ngram in ngram_descriptions:
    process_file(*ngram)
