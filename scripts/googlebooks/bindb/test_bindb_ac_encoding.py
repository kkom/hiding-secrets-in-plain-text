#!/usr/bin/env python3

from pysteg.coding.rational_ac import encode
from pysteg.googlebooks import bindb
from pysteg.googlebooks.ngrams_analysis import normalise_and_explode_tokens
from pysteg.googlebooks.ngrams_analysis import text2token_strings

# Set language model parameters
n = 5
start = 322578
end = 322577
beta = 1
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

# Create the sentence
text = """At the Primorsky polling station in Mariupol, a large crowd is
gathered outside, waiting to vote.  There is a crush of people inside.
 Organisation is chaotic at best.  There are no polling booths: people vote at
the registration desks.  People's details are hastily scribbled on generic
forms.  There is also a collection for money towards funding the Donetsk
People's Republic."""

token_strings = normalise_and_explode_tokens(text2token_strings(text))
token_indices = tuple(map(index.s2i, token_strings))

print(text)
print()
print(" ".join(token_strings))
print()
print(token_indices)
print()

interval = encode(lm.conditional_interval, token_indices, verbose=True)

print("Decoded to: " + str(interval))
