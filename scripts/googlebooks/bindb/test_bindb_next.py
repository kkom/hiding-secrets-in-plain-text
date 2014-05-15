#!/usr/bin/env python3

from pysteg.coding.interval import create_interval
from pysteg.googlebooks import bindb
from pysteg.googlebooks.ngrams_analysis import normalise_and_explode_tokens
from pysteg.googlebooks.ngrams_analysis import text2token_strings

# Set language model parameters
n = 5
start = 322578
end = 322577
beta = 0.5
gamma = 0.1

print("n: {n}".format(**locals()))
print("start: {start}".format(**locals()))
print("end: {end}".format(**locals()))
print("beta: {beta}".format(**locals()))
print("gamma: {gamma}".format(**locals()))
print()

# Load language model
lm = bindb.BinDBLM("/Users/kkom/Desktop/bindb-normalised/counts-consistent-tables", n, start, end, beta, gamma)

# Load index
with open("/Users/kkom/Desktop/bindb-normalised/index", "r") as f:
    index = bindb.BinDBIndex(f)

# Invent a sentence
text = "Hey!  What the fuck is going on here?"
token_strings = normalise_and_explode_tokens(text2token_strings(text))
token_indices = tuple(map(index.s2i, token_strings))

print(text)
print()
print(" ".join(token_strings))
print()
print(token_indices)
print()

# Get the next token after "is" given some intervals
context = token_indices[:9]

intervals = (create_interval(0,1,1000000),
             create_interval(345246,56,1000000),
             create_interval(5465477,322,10000000),
             create_interval(23432566,21,100000000),
             create_interval(10000000000-1000000,1,10000000000))

context_str = " ".join(map(index.i2s, context[-(n-1):]))

for interval in intervals:
    print("Next token given the context \"{context_str}\" and interval {interval}:".format(**locals()))
    next = lm.next(interval, context)
    print(next)

    if next is not None:
        print(index.i2s(next[0]))

    print()
