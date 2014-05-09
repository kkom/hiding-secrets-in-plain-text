#!/usr/bin/env python3

descr = """
This script will make the bindb tables left-consistent, i.e.:

The count of each ngram is greater or equal to the sum of counts of all
(n+1)grams whose first n tokens are equal to the ngram.

Left-consistency is ensured by increasing lower-order ngram counts based on
higher-order counts.
"""

epilog = """
Full consistency is achieved if the count of each ngram was greater or equal to
the sum of counts of all (n+1)grams whose first OR last n tokens are equal to
the ngram.
"""

import argparse
import itertools
import os
import shutil
import struct

from pysteg.common.log import print_status

from pysteg.googlebooks import bindb

def ngrams2left_mgrams(ngrams):
    """
    Given an iterator over ngram counts generate an iterator over left mgrams
    created by integrating over the last token. left mgram is a shortcut for
    the first n-1 tokens of an ngram.
    """

    current_mgram = None
    current_count = 0

    for ngram in ngrams:
        mgram = ngram[0][:-1]
        if mgram == current_mgram:
            current_count += ngram[1]
        else:
            if current_mgram is not None:
                yield (current_mgram, current_count)
            current_mgram = mgram
            current_count = ngram[1]

    if current_mgram is not None:
        yield (current_mgram, current_count)

def process_file(n):
    """Create a left-consistent ngram counts table of order n."""
    ngrams_filename = "{n}gram".format(**locals())
    ngrams_input_path = os.path.join(args.input, ngrams_filename)
    ngrams_output_path = os.path.join(args.output, ngrams_filename)

    # The highest order table is consistent by definition
    if n == args.max_n:
        print_status("Copying", ngrams_input_path, "to", ngrams_output_path)
        shutil.copyfile(ngrams_input_path, ngrams_output_path)
        print_status("Finished copying")
    else:
        # We need to use the already left-consistent table, hence reading from
        # the output directory
        ograms_filename = "{}gram".format(n+1)
        ograms_input_path = os.path.join(args.output, ograms_filename)
        error_path = os.path.join(args.error, ngrams_filename)

        print_status("Checking consistency of", ngrams_input_path)

        with open(ograms_input_path, "rb") as ograms_input, \
             open(ngrams_input_path, "rb") as ngrams_input, \
             open(ngrams_output_path, "wb") as ngrams_output, \
             open(error_path, "w") as error:

            def write_bindb_line(ngram, l):
                """Write a line of the bindb file and return its number."""
                ngrams_output.write(struct.pack(bindb.fmt(n),
                                                *ngram[0]+(ngram[1],)))
                return l+1

            def log_inconsistency(msg):
                print(msg)
                error.write(msg + "\n")

            ngrams = bindb.iter_bindb_file(ngrams_input, n)
            ograms = bindb.iter_bindb_file(ograms_input, n+1)
            integrated_ograms = ngrams2left_mgrams(ograms)

            # If there is no ngram for the integrated ogram, we will find out by
            # reading one ngram too much and realising it comes later than the
            # integrated ogram. We need to keep it in a buffer, so that it get
            # reassessed later.
            ngrams_buffer = tuple()

            l = 0
            for integrated_ogram in integrated_ograms:
                for ngram in itertools.chain(ngrams_buffer, ngrams):
                    if ngram[0] < integrated_ogram[0]:
                        # We haven't yet seen ngram matching the integrated
                        # ogram, simply copy the current ngram to the output
                        l = write_bindb_line(ngram, l)

                    elif ngram[0] == integrated_ogram[0]:
                        # Integrated ogram matches the ngram, ensure that the
                        # ngram count is left-consistent
                        if integrated_ogram[1] <= ngram[1]:
                            l = write_bindb_line(ngram, l)
                        else:
                            l = write_bindb_line(integrated_ogram, l)

                            log_inconsistency(
                                "{l}: {ngram} and integrated {integrated_ogram}"
                                " are inconsistent".format(**locals())
                            )
                        break

                    else:
                        # We have skipped ngram corresponding to the integrated
                        # ogram, need to create it based on the integrated ogram
                        l = write_bindb_line(integrated_ogram, l)

                        # Put in a buffer the ngram that was read ahead
                        ngrams_buffer = iter((ngram,))

                        log_inconsistency("{l}: {integrated_ogram} doesn't"
                                          " exist".format(**locals()))

                        break

        print_status("Saved consistent ngrams to", ngrams_output_path)

# Define and parse arguments
parser = argparse.ArgumentParser(
    description=descr,
    epilog=epilog,
    formatter_class=argparse.RawDescriptionHelpFormatter
)
parser.add_argument("max_n", metavar="n", type=int, help="order of the model")
parser.add_argument("input", help="input directory of inconsistent bindb files")
parser.add_argument("output",
    help="output directory of left-consistent bindb files")
parser.add_argument("error",
    help="directory for the list of inconsistencies fixed")
args = parser.parse_args()

# Process the files
for n in range(args.max_n,0,-1):
    process_file(n)
