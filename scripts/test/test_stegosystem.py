#!/usr/bin/env python3

from pysteg.googlebooks import bindb
from pysteg.stegosystem import BinaryStegosystem, Key, Plaintext, Stegotext

print("Loading index...")
index_path = "/Users/kkom/Desktop/bindb-normalised-no-digits/index"
with open(index_path, "r") as f:
    index = bindb.BinDBIndex(f)

print("Loading language model...")
bindb_dir = "/Users/kkom/Desktop/bindb-normalised-no-digits/counts-consistent-tables"
order = 5
start = index.s2i("_START_")
end = index.s2i("_END_")
beta = 0.1
gamma = 0.01
offset = 35
lm = bindb.BinDBLM(bindb_dir, order, start, end, beta, gamma, offset)

bs = BinaryStegosystem(index, lm)

plaintext = Plaintext(index, lm, "save", "Come in the morning with the documents.")
print("plaintext:", end="\n\n")
print(plaintext, end="\n\n\n\n\n\n")

real_key = Key(index, lm, "save", "Light rain on Monday morning.")
print("real_key:", end="\n\n")
print(real_key, end="\n\n\n\n\n\n")

false_key = Key(index, lm, "save", "Early light rain or drizzle will soon die out.")
print("false_key:", end="\n\n")
print(false_key, end="\n\n\n\n\n\n")

stegotext = bs.pk2s(plaintext, real_key)
print("plaintext encrypted with real_key gives stegotext:", end="\n\n")
print(stegotext, end="\n\n\n\n\n\n")

real_decrypted_plaintext = bs.sk2p(stegotext, real_key)
print("stegotext decrypted with real_key gives real_decrypted_plaintext:", end="\n\n")
print(real_decrypted_plaintext, end="\n\n\n\n\n\n")

false_decrypted_plaintext = bs.sk2p(stegotext, false_key)
print("stegotext decrypted with false_key gives false_decrypted_plaintext:", end="\n\n")
print(false_decrypted_plaintext, end="\n\n\n\n\n\n")
