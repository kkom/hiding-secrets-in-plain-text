#!/usr/bin/env python3

descr = """
This script processes all Google Books Ngrams files listed in a JSON file and
saves them locally.
"""

import argparse
import json
import time

from pysteg.common.streaming import iter_remote_gzip 
from pysteg.google_ngrams.extract_ngram_counts import extract_ngram_counts

# Define and parse the script arguments
parser = argparse.ArgumentParser(
    description=descr,
    formatter_class=argparse.RawDescriptionHelpFormatter
)

parser.add_argument("ngrams", help="JSON file listing all the ngram files")
parser.add_argument("output", help="output directory for processed files")
# parser.add_argument("--off", nargs=2, metavar=("START","END"),
#     help="disables the script between the two hours (HH:MM format)")
    
args = parser.parse_args()

# Read the prefixes
remote_path_root = "http://storage.googleapis.com/books/ngrams/books/"
filename_template = "googlebooks-eng-us-all-{n}gram-20120701-{prefix}.gz"

with open(args.ngrams, 'r') as f:
    ngrams = json.load(f)
    
for n in sorted(ngrams.keys()):
    for prefix in ngrams[n]:
        # Generate filenames
        filename = filename_template.format(n=n, prefix=prefix)
        local_path = os.path.join(args.output, filename)
        
        # If the file is not already done or locked
        if (not os.path.isfile(local_path + "_DONE") and
            not os.path.isfile(local_path + "_LOCK")):
        
            # Generate iterators over ngrams
            source_ngrams = iter_remote_gzip(remote_path_root + filename)
            processed_ngrams = extract_ngram_counts(source_ngrams, int(n))
            
            # Write the file
            open(local_path + "_LOCK", 'w').close()

            with io.open(local_path, 'wb') as f:
                for ngram in processed_ngrams:
                    for word in ngram[0]:
                        f.write(word)
                        f.write(b'\t')
        
                    f.write(bytes(str(ngram[1]), "utf-8"))
                    f.write(b'\n')
            
            os.remove(local_path + "_LOCK")
            open(local_path + "_DONE", 'w').close()
        