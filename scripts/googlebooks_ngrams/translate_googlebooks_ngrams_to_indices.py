#!/usr/bin/env python3

descr = """
This script will translate all words in Google Books Ngrams files to integer
indices.

Each line of the index file line has to consist of an integer and a
corresponding word, separated by a tab.
"""

import argparse
import functools
import json
import multiprocessing
import os

from pysteg.common.files import open_file_to_process, FileAlreadyProcessed
from pysteg.googlebooks import get_partition
from pysteg.googlebooks_ngrams.ngrams_analysis import gen_ngram_descriptions

def read_index(index_file):
    """Read the index file."""
    index = dict(map(
        lambda x: reversed(x[:-1].split('\t')),
        index_file
    ))
    return index

def process_file(descr):
    """Translate words into indices in a single file."""
    n, prefix = descr

    filename_template = "googlebooks-eng-us-all-{n}gram-20120701-{prefix}"
    filename = filename_template.format(**locals())
    
    input_path = os.path.join(args.input, filename)
    output_path = os.path.join(args.output, filename)
    
    partition = get_partition(prefix, partitions)
    
    unindexed = 0
    bad_partition = 0
    
    with open(input_path, "r") as i:
        with open_file_to_process(output_path, "w") as o:
            if o == False:
                raise FileAlreadyProcessed()
            
            for line in i:
                try:
                    l = line.split("\t")
                    l[:-1] = [index[w] for w in l[:-1]]
                    
                    # Check if the first word of the ngram satisfies partition
                    # index constraint 
                    w1 = int(l[0])
                    if (w1 < index_ranges[partition][0]
                            or w1 > index_ranges[partition][1]):
                        print("{l[0]} does not belong to {partition}".format(
                            **locals()))
                        bad_partition += 1
                        continue
                    
                    o.write("\t".join(l))
                except KeyError:
                    # If some word is not in the index (there are only about 10
                    # such words), do not save the ngram. The distribution is
                    # not distorted very much, but it is much easier to
                    # construct the index.
                    print("Unindexed word in line: {line}".format(**locals()),
                        end="")
                    unindexed += 1
            
            print("Translated to {output_path}".format(**locals()))
            
    return (unindexed, bad_partition)

if __name__ == '__main__':
    # Define and parse arguments
    parser = argparse.ArgumentParser(
        description=descr,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("ngrams", help="JSON file listing all the ngram files")
    parser.add_argument("index", type=argparse.FileType('r'), help="index file")
    parser.add_argument("index_ranges",
        help="JSON file listing first word index ranges for each partition")
    parser.add_argument("input", help="input directory of ngram files")
    parser.add_argument("output", help="output directory of ngram files")
    parser.add_argument("--processes", metavar="P", type=int, default=1,
        help="set the number of parallel worker processes (default 1)")
    args = parser.parse_args()

    # Read the index of words
    index = read_index(args.index)
    args.index.close()
    
    # Load index ranges
    with open(args.index_ranges, "r") as f:
        index_ranges = json.load(f)
    partitions = frozenset(index_ranges.keys())

    # Create a generator yielding ngram descriptions
    ngram_descriptions = gen_ngram_descriptions(args.ngrams)
    
    # Process the files
    if args.processes == 1:
        total_unindexed = 0
        total_bad_partition = 0
    
        for ngram in ngram_descriptions:
            (unindexed, bad_partition) = process_file(ngram)
            total_unindexed += unindexed
            total_bad_partition += bad_partition
    else:
        p = multiprocessing.Pool(processes=args.processes)
        errors = p.map(process_file, ngram_descriptions, 1)
        p.close()
        p.join()

        (total_unindexed, total_bad_partition) = functools.reduce(
                lambda x, y: (x[0]+y[0], x[1]+y[1]),
                errors
            )
            
    print("Ngrams with unindexed words discarded: {total_unindexed}".format(
        **locals()))
    print("Ngrams in bad partitions discarded: {total_bad_partition}".format(
        **locals()))
