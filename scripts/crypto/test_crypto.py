#!/usr/bin/env python3

from pysteg.crypto import decrypt
from pysteg.crypto import encrypt
from pysteg.coding.interval import bits2interval, interval2bits
from pysteg.coding.rational_ac import decode, deep_decode, encode
from pysteg.googlebooks import bindb
from pysteg.googlebooks.ngrams_analysis import normalise_and_explode_tokens
from pysteg.googlebooks.ngrams_analysis import text2token_strings
from pysteg.googlebooks.ngrams_analysis import token_strings2text

# Set language model parameters
n = 5
start = 322578
end = 322577
alpha = 0.1
beta = 0.01

# Load language model
print("Loading language model...")
lm = bindb.BinDBLM("/Users/kkom/Desktop/bindb-normalised/counts-consistent-tables", n, start, end, alpha, beta)

# Load index
print("Loading words index...")
with open("/Users/kkom/Desktop/bindb-normalised/index", "r") as f:
    index = bindb.BinDBIndex(f)

# Invent plaintext
plaintext = "who do you miss the most?"
plaintext_strings = normalise_and_explode_tokens(text2token_strings(plaintext))
plaintext_indices = tuple(map(index.s2i, plaintext_strings))

print("Plaintext: " + plaintext, end="\n\n")
print("Plaintext token strings: " + str(plaintext_strings), end="\n\n")
print("Plaintext token indices: " + str(plaintext_indices), end="\n\n")

# Invent a password
password = "Marion"
password_strings = normalise_and_explode_tokens(text2token_strings(password))
password_indices = tuple(map(index.s2i, password_strings))

print("Password: " + password, end="\n\n")
print("Password token strings: " + str(password_strings), end="\n\n")
print("Password token indices: " + str(password_indices), end="\n\n")

# Map plaintext and password symbol sequences to intervals
plaintext_interval = encode(lm.conditional_interval, plaintext_indices)
print("Plaintext interval: " + str(plaintext_interval), end="\n\n")
password_interval = encode(lm.conditional_interval, password_indices)
print("Password interval: " + str(password_interval), end="\n\n")

# Convert plaintext and password intervals to bit sequences
plaintext_bits = interval2bits(plaintext_interval, "sub")
print("Plaintext bits (a subinterval of plaintext interval): " + str(plaintext_bits), end="\n\n")
password_bits = interval2bits(password_interval, "super")
print("Password bits (the smallest superinterval of password interval): " + str(password_bits), end="\n\n")

# Encrypt plaintext bits to ciphertext bits
ciphertext_bits = encrypt(plaintext_bits, password_bits, verbose=True)
print("Ciphertext bits: " + str(ciphertext_bits), end="\n\n")

# Convert ciphertext bits to stegotext interval
stegotext_interval = bits2interval(ciphertext_bits)
print("Stegotext interval: " + str(stegotext_interval), end="\n\n")

# Map stegotext interval to stegotext symbol sequence (deep)
stegotext_indices = deep_decode(lm.next, stegotext_interval, end=end)
print("Stegotext token indices: " + str(stegotext_indices), end="\n\n")
stegotext_strings = tuple(map(index.i2s, stegotext_indices))
print("Stegotext token strings: " + str(stegotext_strings), end="\n\n")
stegotext_text = token_strings2text(stegotext_strings)
print("Output stegotext: " + stegotext_text, end="\n\n")

# Parse stegotext string to indices
parsed_stegotext_strings = normalise_and_explode_tokens(text2token_strings(stegotext_text))
parsed_stegotext_indices = tuple(map(index.s2i, parsed_stegotext_strings))
print("Parsed stegotext token indices: " + str(parsed_stegotext_indices), end="\n\n")

# Encode stegotext indices to an interval
parsed_stegotext_interval = encode(lm.conditional_interval, parsed_stegotext_indices)
print("Parsed stegotext interval: " + str(parsed_stegotext_interval), end="\n\n")

# Convert stegotext interval to stegotext bits
parsed_stegotext_bits = interval2bits(parsed_stegotext_interval, "super")
print("Parsed stegotext bits (the smallest superinterval of the parsed stegotext interval): " + str(parsed_stegotext_bits), end="\n\n")

# Decrypt stegotext bits
decrypted_plaintext_bits = decrypt(parsed_stegotext_bits, password_bits)
print("Decrypted plaintext bits: " + str(decrypted_plaintext_bits), end="\n\n")

# Convert decrypted plaintext bits to plaintext interval
decrypted_plaintext_interval = bits2interval(decrypted_plaintext_bits)
print("Decrypted plaintext interval: " + str(decrypted_plaintext_interval), end="\n\n")

# Get decrypted plaintext token indices and strings
decrypted_plaintext_token_indices = decode(lm.next, decrypted_plaintext_interval)
print("Decrypted plaintext token indices: " + str(decrypted_plaintext_token_indices), end="\n\n")
decrypted_plaintext_strings = tuple(map(index.i2s, decrypted_plaintext_token_indices))
print("Decrypted plaintext token strings: " + str(decrypted_plaintext_strings), end="\n\n")
decrypted_plaintext_text = token_strings2text(decrypted_plaintext_strings)
print("Decrypted plaintext: " + decrypted_plaintext_text, end="\n\n")
