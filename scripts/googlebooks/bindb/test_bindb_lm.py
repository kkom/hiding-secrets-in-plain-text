#!/usr/bin/env python3

from sympy import log, N, Rational

from pysteg.googlebooks.ngrams_analysis import normalise_and_explode_tokens
from pysteg.googlebooks.ngrams_analysis import text2token_strings

from pysteg.googlebooks import bindb

alpha = 0.5
beta = 0.1

print("alpha: {alpha}".format(**locals()))
print("beta: {beta}".format(**locals()))

lm = bindb.BinDBLM("/Users/kkom/Desktop/bindb-normalised/counts-consistent-tables", 5, 322578, 322577, alpha, beta)

ngram = (322578, 587568, 4223883, 3455033)
token = ngram[-1]
context = ngram[:-1]

with open("/Users/kkom/Desktop/bindb-normalised/index", "r") as f:
    index = bindb.BinDBIndex(f)

text = """
"No, not exactly because of it," answered Porfiry.  "In his article all
men are divided into 'ordinary' and 'extraordinary.'  Ordinary men have
to live in submission, have no right to transgress the law, because,
don't you see, they are ordinary.  But extraordinary men have a right to
commit any crime and to transgress the law in any way, just because they
are extraordinary.  That was your idea, if I am not mistaken?"

"What do you mean?  That can't be right?"  Razumihin muttered in
bewilderment.

Raskolnikov smiled again.  He saw the point at once, and knew where they
wanted to drive him.  He decided to take up the challenge.
"""

text = """
At the Primorsky polling station in Mariupol, a large crowd is gathered outside,
waiting to vote.  There is a crush of people inside.  Organisation is chaotic at
best.  There are no polling booths: people vote at the registration desks.
 People's details are hastily scribbled on generic forms.  There is also a
collection for money towards funding the Donetsk People's Republic.
"""

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
    context_string = " ".join((token_strings[:i])[-4:])

    print("P({token_string} | {context_string}) = {interval} with entropy {entropy}".format(**locals()))

total_entropy = sum(entropies)
entropy_per_character = total_entropy / len(text)
entropy_per_token = total_entropy / len(token_strings)

print()
print("total interval size: {total_size}".format(**locals()))
print("total entropy: {total_entropy}".format(**locals()))
print("bits per character: {entropy_per_character}".format(**locals()))
print("bits per word: {entropy_per_token}".format(**locals()))
