#!/usr/bin/env python3

descr = """
This script will show the size of all Google Books Ngrams files.
"""

import argparse
import json
import urllib.parse
import urllib.request

from pysteg.googlebooks.ngrams_analysis import gen_ngram_descriptions

def get_ngram_file_size(ngram_description):
    """
    Get the size in bytes of a compressed Google Books Ngram in a remote
    location.
    """

    (n, prefix) = ngram_description

    remote_root = "http://storage.googleapis.com/books/ngrams/books/"
    filename_template = "googlebooks-eng-us-all-{n}gram-20120701-{prefix}.gz"
    filename = filename_template.format(n=n, prefix=prefix)
    remote_path = urllib.parse.urljoin(remote_root, filename)

    with urllib.request.urlopen(remote_path) as f:
        size = f.info()["Content-Length"]

    return size

if __name__ == '__main__':
    # Define and parse the script arguments
    parser = argparse.ArgumentParser(description=descr)
    parser.add_argument("ngrams", help="JSON file listing all the ngram files")
    args = parser.parse_args()

    # Create the iterator for ngram descriptions
    ngram_descriptions = gen_ngram_descriptions(args.ngrams)

    # Print the column headers
    print("{n} {prefix:11} {size_gb:>9} {cum_size_gb:>15}".format(
        n = "n",
        prefix = "prefix",
        size_gb = "size (GB)",
        cum_size_gb = "cum size (GB)"
    ))

    # Print the sizes and cumulative sizes in GB
    cum_size = 0
    for ngram in ngram_descriptions:
        (n, prefix) = ngram
        size = int(get_ngram_file_size(ngram))
        cum_size += size

        size_gb = size / pow(1024,3)
        cum_size_gb = cum_size / pow(1024,3)

        print("{n} {prefix:11} {size_gb:9.1f} {cum_size_gb:15.1f}".format(**locals()))
