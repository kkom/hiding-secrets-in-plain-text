import itertools
import operator
import random

class PasswordTooShortError(Exception):
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

def encrypt(plaintext, password, strict=False, verbose=False):
    """
    Encrypt an information bit sequence using a password bit sequence.

    The password bit sequence is used as a one-time pad. In strict mode an
    Exception is thrown if the password is shorter than the information
    sequence.

    The password is simply cycled if it is of insufficient length. Thus
    knowledge of the beginning of the secret sequence allows to trivially guess
    the rest of the generated password and the secret message.
    """

    if len(password) < len(plaintext):
        if verbose: print("Password too short to perfectly encrypt plaintext!")
        if strict: raise PasswordTooShortError()

    repeated_password = itertools.cycle(password)

    return tuple(map(operator.xor, plaintext, repeated_password))

def decrypt(ciphertext, password):
    """
    Since the encryption method of encrypt is XOR, it suffices to encrypt the
    ciphertext with password to recover plaintext.
    """
    return encrypt(ciphertext, password, strict=False, verbose=False)
