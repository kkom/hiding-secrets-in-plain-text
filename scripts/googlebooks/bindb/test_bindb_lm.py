#!/usr/bin/env python3

from sympy import log, N, Rational

from pysteg.googlebooks import bindb

from pysteg.googlebooks.ngrams_analysis import normalise_and_explode_tokens
from pysteg.googlebooks.ngrams_analysis import text2token_strings

# Set language model parameters
n = 5
start = 322578
end = 322577
alpha = 0.5
beta = 0.1

print("n: {n}".format(**locals()))
print("start: {start}".format(**locals()))
print("end: {end}".format(**locals()))
print("alpha: {alpha}".format(**locals()))
print("beta: {beta}".format(**locals()))
print()

# Load language model
lm = bindb.BinDBLM("/Users/kkom/Desktop/bindb-normalised/counts-consistent-tables", n, start, end, alpha, beta)

# Load index
with open("/Users/kkom/Desktop/bindb-normalised/index", "r") as f:
    index = bindb.BinDBIndex(f)

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

total_size = Rational(1)
intervals = []
entropies = []
for i in range(len(token_indices)):
    token = token_indices[i]
    context = token_indices[:i]
    interval = lm.conditional_interval(token, context)
    total_size = total_size * interval.l
    entropy = N(-log(interval.l, 2))

    intervals.append(interval)
    entropies.append(entropy)

    token_string = token_strings[i]
    context_string = " ".join((token_strings[:i])[-(n-1):])

    print("P({token_string} | {context_string}) = {interval} with entropy {entropy}".format(**locals()))

total_entropy = sum(entropies)
entropy_per_character = total_entropy / len(text)
entropy_per_token = total_entropy / len(token_strings)

print()
print("total interval size: {total_size}".format(**locals()))
print("total entropy: {total_entropy}".format(**locals()))
print("bits per character: {entropy_per_character}".format(**locals()))
print("bits per word: {entropy_per_token}".format(**locals()))
