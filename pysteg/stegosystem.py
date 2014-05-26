import pprint

import sympy

from pysteg.coding.interval import bits2interval, interval2bits
from pysteg.coding.rational_ac import decode, deep_decode, encode
from pysteg.crypto import random_bits, decrypt, encrypt
from pysteg.googlebooks.ngrams_analysis import normalise_and_explode_tokens
from pysteg.googlebooks.ngrams_analysis import text2token_strings
from pysteg.googlebooks.ngrams_analysis import token_strings2text

class Sentence:
    """
    A sentence can be created in multiple ways:
    """

    def __init__(self, index, lm, input, input_type, binary_interval,
                 decode_mode=None):
        """Initialise the sentence either from text or from an interval."""

        assert(input_type in ("text", "interval"))
        assert(binary_interval in (None, "sub", "super"))

        self.index = index
        self.lm = lm

        if input_type == "text":
            text = input
            raw_token_strings = text2token_strings(text)
            self.token_strings = normalise_and_explode_tokens(raw_token_strings)
            self.token_indices = tuple(map(self.index.s2i, self.token_strings))
            self.interval = encode(lm.conditional_interval, self.token_indices)
            if binary_interval:
                self.bits = interval2bits(self.interval, binary_interval)
        elif input_type == "interval":
            assert(decode_mode in ("deep", "shallow"))
            start_interval = input
            if decode_mode == "deep":
                decoding_result = deep_decode(lm.next, start_interval,
                                              end=self.index.s2i("_END_"))
            elif decode_mode == "shallow":
                decoding_result = decode(lm.next, start_interval)
            self.token_indices = decoding_result.sequence
            self.token_strings = tuple(map(self.index.i2s, self.token_indices))
            self.interval = decoding_result.interval
            if binary_interval:
                self.bits = interval2bits(self.interval, binary_interval)

    def __str__(self):
        entropy = float(sympy.N(-sympy.log(self.interval.l, 2)))

        description = {}
        description["Type"] = type(self).__name__
        description["Token indices"] = " ".join(map(str, self.token_indices))
        description["Token strings"] = " ".join(self.token_strings)
        description["Interval"] = str(self.interval)
        description["Entropy"] = "{:.1f} bits".format(entropy)
        description["Binary representation"] = " ".join(map(str, self.bits))
        description["Binary representation entropy"] = "{} bits".format(
            len(self.bits))
        description["Text representation"] = token_strings2text(self.token_strings)

        return(pprint.pformat(description))

class Key(Sentence):

    def __init__(self, index, lm, mode, input):
        """
        Generate key with a given minimum number of bits or create it from
        supplied text.
        """

        assert(mode in ("generate", "save"))

        if mode == "generate":
            min_number_of_bits = input
            start_interval = bits2interval(random_bits(min_number_of_bits))
            super().__init__(index, lm, start_interval, input_type="interval",
                             binary_interval="super", decode_mode="deep")
        elif mode == "save":
            super().__init__(index, lm, input, input_type="text",
                             binary_interval="super")

class Plaintext(Sentence):

    def __init__(self, index, lm, mode, input):
        """
        Initialise plaintext either from bits decrypted from stegotext or
        directly from text.
        """

        assert(mode in ("decode_bits", "save"))

        if mode == "decode_bits":
            self.bits = input
            interval = bits2interval(self.bits)
            super().__init__(index, lm, interval, input_type="interval",
                             binary_interval=None, decode_mode="shallow")
        elif mode == "save":
            super().__init__(index, lm, input, input_type="text",
                             binary_interval="sub")

class Stegotext(Sentence):

    def __init__(self, index, lm, mode, input):
        """
        Initialise stegotext either from bits encrypted from plaintext or
        directly from text.
        """

        assert(mode in ("decode_bits", "save"))

        if mode == "decode_bits":
            bits = input
            interval = bits2interval(bits)
            super().__init__(index, lm, interval, input_type="interval",
                             binary_interval="super", decode_mode="deep")
        elif mode == "save":
            super().__init__(index, lm, input, input_type="text",
                             binary_interval="super")

class BinaryStegosystem:
    """
    Stegosystem based on mapping sentences to intervals and then to bit
    sequences.
    """

    def __init__(self, index, lm):
        self.index = index
        self.lm = lm

    def pk2s(self, p, k):
        """Encrypt plaintext with key and return encrypted stegotext."""

        bits = encrypt(p.bits, k.bits)
        return Stegotext(index, lm, "decode_bits", bits)

    def sk2p(self, s, k):
        """Decrypt encrypted stegotext with key and return plaintext."""

        bits = decrypt(s.bits, k.bits)
        return Plaintext(index, lm, "decode_bits", bits)
