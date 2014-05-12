def decode(next, interval):
    """Decode an interval using the supplied next token function."""

    context = []
    search_result = next(interval, tuple(context))

    while search_result is not None:
        print(search_result[0])
        context.append(search_result[0])
        search_result = next(search_result[1], tuple(context))

    return context
