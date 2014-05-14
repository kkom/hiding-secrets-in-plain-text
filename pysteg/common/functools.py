def converge(f, x, limit=float("inf")):
    """Repeatedly apply f to x until the result does not change."""

    fx = f(x)
    i = limit-1

    while fx != x and i > 0:
        x = fx
        fx = f(x)
        i -= 1
    return fx
