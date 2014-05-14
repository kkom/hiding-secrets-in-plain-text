def converge(f, x, limit=0):
    """Repeatedly apply f to x until the result does not change."""

    fx = f(x)
    while fx != x:
        x = fx
        fx = f(x)
    return fx
