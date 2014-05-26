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
lm = bindb.BinDBLM(bindb_dir, order, start, end, beta, gamma)

bs = BinaryStegosystem(index, lm)
p = {}
k = {}
s = {}
dp = {}
fdp = {}

p[1] = Plaintext(index, lm, "save", "Kim and Kanye wed in Florence.")
print("p[1]:", end="\n\n")
print(p[1], end="\n\n\n\n\n\n")

k[1] = Key(index, lm, "save", "Fog patches or mist may form around midnight.")
print("k[1]:", end="\n\n")
print(k[1], end="\n\n\n\n\n\n")

k[2] = Key(index, lm, "generate", 64)
print("automatically generated k[2]:", end="\n\n")
print(k[2], end="\n\n\n\n\n\n")

k[3] = Key(index, lm, "save", "Obama makes surprise Afghan visit.")
print("fake k[3]:", end="\n\n")
print(k[3], end="\n\n\n\n\n\n")

s[1] = bs.pk2s(p[1], k[1])
print("p[1] encrypted with k[1] gives s[1]:", end="\n\n")
print(s[1], end="\n\n\n\n\n\n")

s[2] = bs.pk2s(p[1], k[2])
print("p[1] encrypted with k[2] gives s[2]:", end="\n\n")
print(s[2], end="\n\n\n\n\n\n")

dp[1] = bs.sk2p(s[1], k[1])
print("s[1] decrypted with k[1] gives dp[1]:", end="\n\n")
print(dp[1], end="\n\n\n\n\n\n")

dp[2] = bs.sk2p(s[2], k[2])
print("s[2] decrypted with k[2] gives dp[2]:", end="\n\n")
print(dp[2], end="\n\n\n\n\n\n")

fdp[1] = bs.sk2p(s[1], k[3])
print("s[1] decrypted with k[3] gives fdp[1]:", end="\n\n")
print(fdp[1], end="\n\n\n\n\n\n")
