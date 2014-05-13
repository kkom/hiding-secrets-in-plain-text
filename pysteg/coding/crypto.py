import itertools
import operator
import random

class KeyTooShortError(Exception):
    pass

def random_bits(length, seed=None):
    """
    Return a sequence of cryptographically secure random bits, i.e. generated
    using the os.urandom() function. Optionally generate them using a specific
    seed - this option is provided for analysing the system deterministically.
    """

    if seed is None:
        rng = random.SystemRandom()
    else:
        rng = random
        random.seed(seed)

    return tuple(rng.randrange(2) for i in range(length))

def encrypt(plaintext, key, strict=False, verbose=False):
    """
    Encrypt a plaintext bit sequence using a key bit sequence.

    In strict mode, the key is used as a one-time pad and an exception is thrown
    if the key is shorter than plaintext.

    In non-strict mode, the key is cycled to create a simple stream cipher.
    """

    if len(key) < len(plaintext):
        if verbose: print("Key too short for a one-time pad cipher!")
        if strict: raise KeyTooShortError()

    return tuple(map(operator.xor, plaintext, itertools.cycle(key)))

def decrypt(ciphertext, key):
    """
    Since the encryption method combines plaintext and key using a XOR
    operation, to recover plaintext it suffices to re-encrypt ciphertext with
    the key.
    """
    return encrypt(ciphertext, key, strict=False, verbose=False)
