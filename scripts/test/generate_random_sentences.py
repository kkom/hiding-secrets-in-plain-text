#!/usr/bin/env python3

"""
Use the Key
"""

import sympy

from pysteg.googlebooks import bindb
from pysteg.stegosystem import Key

print("Loading index...")
index_path = "/Users/kkom/Desktop/bindb-normalised-no-digits/index"
with open(index_path, "r") as f:
    index = bindb.BinDBIndex(f)

bindb_dir = "/Users/kkom/Desktop/bindb-normalised-no-digits/counts-consistent-tables"
start = index.s2i("_START_")
end = index.s2i("_END_")

def generate_sentences(order, beta, gamma):
    """Generate sentences with appropriate beta and gamma."""

    print("Loading language model...")
    lm = bindb.BinDBLM(bindb_dir, order, start, end, beta, gamma)

    # Generate the sentences by generating a random key
    return Key(index, lm, "generate", 4096)

orders = (5,4,3,2)
params = ((1,0), (0.5,0.05), (0.1,0.01), (0.01,0.001), (0,0.4), (0,0.1), (0,0.05))

for order in orders:
    for param in params:
        print("order {order}, params {param}".format(**locals()), end="\n\n")

        k = generate_sentences(order, *param)

        # Print token strings
        print(k.token_strings, end="\n\n")
        print(" ".join(k.token_strings), end="\n\n")

        # Count non-obvious tokens. _START_ tokens are actually unnecessary --
        # they are guaranteed to occur at the beginning of the whole message and
        # after every _END_ -- they do not shrink the interval. They are
        # included as separate tokens only for implementation reasons. Therefore
        # the sentence length
        sentence_count = k.token_strings.count("_START_")
        non_obvious_tokens = len(k.token_strings) - sentence_count

        # Print interval entropy and perplexity
        entropy = float(sympy.N(-sympy.log(k.interval.l, 2)))
        perplexity = entropy / non_obvious_tokens
        print("Sequence entropy: {entropy}".format(**locals()))
        print("Sequence perplexity: {perplexity}".format(**locals()))

        average_sentence_length = non_obvious_tokens / sentence_count
        print("Average sentence length: {average_sentence_length}".format(**locals()), end="\n\n\n")
