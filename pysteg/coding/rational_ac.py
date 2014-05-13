import sympy

from pysteg.coding.interval import create_interval
from pysteg.coding.interval import find_subinterval
from pysteg.coding.interval import is_subinterval
from pysteg.coding.interval import randomly_refine
from pysteg.coding.interval import scale_interval

def decode(next, interval, verbose=False):
    """
    Decode an interval using the supplied "next token" function. Decoding ends
    when interval corresponding to the output sequence is the smallest
    superinterval of the input interval.
    """

    sequence = []
    search_result = next(interval, tuple(sequence))

    while search_result is not None:
        if verbose: print(search_result[0])
        sequence.append(search_result[0])
        search_result = next(search_result[1], tuple(sequence))

    return tuple(sequence)

def deep_decode(next, i, end=None, verbose=False):
    """
    Decode an interval using a randomly chosen refinement of the input interval
    and the supplied "next token" function. The input interval is refined until
    the interval corresponding to the output sequence is its subinterval. The
    output sequence is optionally repeated until a specified end token is
    generated.
    """

    # i   - actual input interval
    # ir  - refined input interval
    # o   - output sequence interval
    # irs - refined input interval scaled as a part of the output interval

    # Number of random bits to be appended during each input interval refinement
    n = 2

    ir = randomly_refine(n, i, seed=1)
    output_sequence = []

    search_result = next(ir, tuple(output_sequence))

    while search_result is not None:
        # The search procedure gives us the next token and the refined input
        # interval scaled inside the current output interval
        (token, irs) = search_result

        output_sequence.append(token)

        # Calculate the current output interval using the refined input interval
        # scaled to it and the knowledge of actual refined input interval
        o = create_interval(
            ir.b - sympy.Rational(irs.b * ir.l, irs.l),
            ir.l / irs.l
        )

        if verbose: print(token), print(o)

        # Terminate generating the output sequence if the output interval
        # becomes a subinterval of the actual input interval. Optionally wait
        # for generating the end token.
        if is_subinterval(i, o) and (end is None or token == end):
            break

        search_result = next(irs, tuple(output_sequence))

    return tuple(output_sequence)

def encode(conditional_interval, sequence, verbose=False):
    """
    Encode a sequence into an exact interval using the supplied "conditional
    subinterval" function.
    """

    interval = create_interval(0,1)

    for i in range(len(sequence)):
        if verbose: print(sequence[i])
        interval = find_subinterval(
            interval, conditional_interval(sequence[i], sequence[:i])
        )

    return interval
