#!/usr/bin/env python3

descr = """
This script will translate all words in Google Books Ngrams files to integer
indices.

Each line of the index file line has to consist of an integer and a
corresponding word, separated by a tab.
"""

import argparse
import multiprocessing
import os

from pysteg.common.files import open_file_to_process, FileAlreadyProcessed
from pysteg.googlebooks_ngrams.ngrams_analysis import gen_ngram_descriptions

def read_index(index_file):
    """Read the index file."""
    index = dict(map(
        lambda x: reversed(x[:-1].split('\t')),
        index_file
    ))
    index["_START_"] = "-1"
    index["_END_"] = "-2"
    return index

def process_file(descr):
    """Translate words into indices in a single file."""
    n, prefix = descr

    filename_template = "googlebooks-eng-us-all-{n}gram-20120701-{prefix}"
    filename = filename_template.format(**locals())
    
    input_path = os.path.join(args.input, filename)
    output_path = os.path.join(args.output, filename)
    
    with open(input_path, "r") as i:
        with open_file_to_process(output_path, "w") as o:
            if o == False:
                raise FileAlreadyProcessed()
                
            print("Translating from {input_path}".format(**locals()))
            
            for line in i:
                l = line.split("\t")
                l[:-1] = [index[w] for w in l[:-1]]
                o.write("\t".join(l))
            
            print("Translated to {output_path}".format(**locals()))

if __name__ == '__main__':
    # Define and parse arguments
    parser = argparse.ArgumentParser(
        description=descr,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("ngrams", help="JSON file listing all the ngram files")
    parser.add_argument("index", type=argparse.FileType('r'), help="index file")
    parser.add_argument("input", help="input directory of ngram files")
    parser.add_argument("output", help="output directory of ngram files")
    parser.add_argument("--processes", metavar="P", type=int, default=1,
        help="set the number of parallel worker processes (default 1)")
    args = parser.parse_args()

    # Read the index of words
    index = read_index(args.index)
    args.index.close()

    # Create a generator yielding ngram descriptions
    ngram_descriptions = gen_ngram_descriptions(args.ngrams)
    
    # Process the files
    if args.processes == 1:
        for ngram in ngram_descriptions:
            process_file(ngram)
    else:
        p = multiprocessing.Pool(processes=args.processes)
        p.map(process_file, ngram_descriptions, 1)
        p.close()
        p.join()
