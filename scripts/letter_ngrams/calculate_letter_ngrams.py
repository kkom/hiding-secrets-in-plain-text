#!/usr/bin/env python3

descr = """
This script creates a histogram of letter n-grams from text files. The n-grams
include 26 ASCII letters, digits and the following punctuation: !"'(),-.:;?

All whitespace is converted to a single space and can occur only in the
beginning or end of an n-gram.
"""

import argparse
import string

from unidecode import unidecode

def update_ngrams(hist, N, text):
    """Write docs here."""
        
    def new_buffer():
        return ([[' ' for j in range(i+1)] for i in range(N)], 1)
    
    solid = string.digits + string.ascii_lowercase + '!"\'(),-.:;?'
    whitespace = string.whitespace
    all = solid + whitespace
    
    with open(text, 'r') as f:
        for r in f:
            (b,k) = new_buffer()
            
            for l in unidecode(r).lower():
                if l in all:
                    if l in whitespace:
                        l = ' '
                
                    for n in range(N):
                        if k <= n:
                            b[n][k] = l
                        else:
                            b[n] = b[n][1:] + [l]
                        
                        if k >= n:
                            s = ''.join(b[n])
                            hist[n][s] = hist[n].get(s,0) + 1
                            
                    k += 1
                    
                    if l in whitespace:
                        (b,k) = new_buffer()
    return hist

# Define and use the parser
parser = argparse.ArgumentParser(
    description=descr,
    formatter_class=argparse.RawDescriptionHelpFormatter
)
parser.add_argument("N", type=int, help="order of the n-grams")
parser.add_argument("output", help="location to save the output n-grams")
parser.add_argument("input", nargs='+', help="source text files")
args = parser.parse_args()

# Initialise the histogram of n-grams
hist = [{} for i in range(args.N)]

# Update the histogram of n-grams
for text in args.input:
    hist = update_ngrams(hist, args.N, text)
    print('Processed {}'.format(text))

# Save the result to text files
for n in range(args.N):
    with open("{name}_{n}".format(name=args.output,n=n+1), "w") as f:
        for k,v in sorted(hist[n].items()):
            f.write("{0}\t{1}\n".format(k,v))
