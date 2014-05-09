#!/usr/bin/env python3

from sympy import Rational

from pysteg.googlebooks_ngrams.psql import GooglebooksNgramsLanguageModel

def show(n1, n2):
    print(float(n1), float(n2))

lm = GooglebooksNgramsLanguageModel("steganography", "googlebooks", 5)

w = 4292446
context = (4599888,6835529,2722355,6835529)

c1, c2 = lm.c(w, context)

show(c1,c2)

new_c1 = c1 + Rational(1,4) * (c2-c1)
new_c2 = c2

bad_c2 = c1 + Rational(5,4) * (c2-c1)

show(new_c1,new_c2)
print(lm.next((new_c1, new_c2), context))

show(new_c1,bad_c2)
print(lm.next((new_c1, bad_c2), context))
