def take(l, n):
    """
    Take the first n (if n > 0) or last -n (if n < 0) elements of a sequence.

    It is necessary to construct a separate function rather than use the slicing
    mechanism, as it is impossible to select zero last elements by doing l[-0:].
    """

    if n >= 0:
        return l[:n]
    else:
        return l[n:]
