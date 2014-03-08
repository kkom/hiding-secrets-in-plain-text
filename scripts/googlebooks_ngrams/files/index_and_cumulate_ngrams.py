#!/usr/bin/env python3

descr = """
This script will translate all words in Google Books Ngrams files to integer
indices, sort them alphabetically and save in a binary file including the
cumulative frequencies.

Each line of the index file line has to consist of an integer and a
corresponding word, separated by a tab.
"""

import argparse
import json
import struct

from itertools import chain, count
from os import path

from pysteg.googlebooks2 import create_partition_names
from pysteg.googlebooks2 import get_partition
from pysteg.googlebooks2 import partition_order
from pysteg.googlebooks_ngrams.ngrams_analysis import ngram_filename

def write_ngrams_table(n, prefixes):
    """Writes a cumulative frequencies ngrams table for a particular n."""
    
    # This is a very messy piece of code which constructs the schedule of
    # partitions and ngram files. The schedule is an order list of tuples
    # containing an ordered list of partitions to which words need to be
    # assigned and an unordered list of ngram files from which ngrams are
    # assigned to any of those partitions. The induced order of partitions needs
    # to follow the general order of partitions, in order to produce ordered
    # cumulative ngram tables.
    partition_names = create_partition_names();
    if n == 1:
        # Create a raw schedule dictionary keyed by prefix files
        raw_schedule = { (pref,):set() for pref in prefixes
                         if pref not in {"other", "punctuation"} }
        
        # Assign partitions to prefixes
        for part in partition_names:
            if part != "_":
                raw_schedule[(part[0],)].add(part)
                
        # Handle the special case of "other"/"punctuation" and "_"
        raw_schedule[("other", "punctuation")] = {"_"}
        
        # Convert raw schedule into a list of partition-prefix tuples
        pp = ((part, frozenset(pref)) for pref, part in raw_schedule.items())
        
    else:
        # Create a raw schedule dictionary keyed by partition names
        raw_schedule = {(part,):set() for part in partition_names}
        
        # Assign prefixes to partitions
        for pref in prefixes:
            if pref not in {"other", "punctuation"}:
                raw_schedule[(get_partition(pref),)].add(pref)
        
        # Handle the special case of "other"/"punctuation" and "_"
        raw_schedule[("_",)] = {"other", "punctuation"}
        
        # Convert raw schedule into a list of partition-prefix tuples
        pp = ((part, frozenset(pref)) for part, pref in raw_schedule.items())
    
    # Sort the schedule
    schedule = tuple(sorted(
        ( (tuple(sorted(part,key=partition_order)), pref) for part, pref in pp),
        key=lambda schedule_pair: partition_order(schedule_pair[0][0])
    ))
    
    # Create the file with all ngrams
    with open(path.join(args.output, "{n}gram".format(**locals())), "wb") as fn:
        # Prepare the line format specifier
        fmt = "<" + n * "i" + "q"
    
        # Write the first line consisting of all columns equal to 0
        cf = 0
        fn.write(struct.pack(fmt, *n*(0,) + (cf,)))
    
        # Go over the schedule
        for parts, prefs in schedule:    
            # Create a dictionary of ngram sets separated by partitions
            partitions = {part:set() for part in parts}
            partitions_set = frozenset(parts)
            
            # Read files one by one
            for pref in prefs:
                filename = ngram_filename(n,pref)
                with open(path.join(args.input, filename), "r") as fp:
                    with open(path.join(args.error, filename), "w") as fe:                  
                        for line in fp:
                            ngram = line[:-1].split("\t")
                            try:
                                # Translate all words into their indices
                                ixs = tuple(map(lambda x: w2i[x], ngram[:-1]))
                                # Partition in which the ngram should fall
                                part = get_partition(ngram[0], partitions_set)
                                # Add the ngram
                                partitions[part].add((ixs, int(ngram[-1])))
                            except KeyError:
                                fe.write(line)
                        print("Read ngrams from {filename}".format(**locals()))
            
            # Sort and dump the partitions
            for part in partitions:
                for ngram in sorted(partitions[part]):
                    print(ngram)
                    cf += ngram[1]
                    fn.write(struct.pack(fmt, *ngram[0] + (cf,)))
                print("Dumped {part} to {n}gram file".format(**locals()))

if __name__ == '__main__':
    # Define and parse arguments
    parser = argparse.ArgumentParser(
        description=descr,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("ngrams", help="JSON file listing all the ngram files")
    parser.add_argument("index", help="index file")
    parser.add_argument("input", help="input directory of ngram files")
    parser.add_argument("output", help="output directory of ngram files")
    parser.add_argument("error", help="output directory for discarded ngrams")
    args = parser.parse_args()

    # Read the index of words
    with open(args.index, "r") as f:
        w2i = dict(zip(
            map(lambda x: x[:-1].split("\t")[1], f),
            count(1)
        ))
    
    # Load the ngram descriptions
    with open(args.ngrams, "r") as f:
         ngram_descriptions = json.load(f)
    
    for n, prefixes in ngram_descriptions.items():
        write_ngrams_table(int(n), prefixes)
        break
