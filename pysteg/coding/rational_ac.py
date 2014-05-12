from pysteg.coding.interval import create_interval, find_subinterval

def decode(next, interval, verbose=False):
    """Decode an interval using the supplied next token function."""

    sequence = []
    search_result = next(interval, tuple(sequence))

    while search_result is not None:
        if verbose: print(search_result[0])
        sequence.append(search_result[0])
        search_result = next(search_result[1], tuple(sequence))

    return tuple(sequence)

def encode(conditional_interval, sequence, verbose=False):
    """Encode a sequence using the supplied conditional interval function."""

    interval = create_interval(0,1)

    for i in range(len(sequence)):
        if verbose: print(sequence[i])
        interval = find_subinterval(
            interval, conditional_interval(sequence[i], sequence[:i])
        )

    return interval
