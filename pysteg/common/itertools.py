import collections

from itertools import chain, islice

FilteredTuple = collections.namedtuple('FilteredTuple', 'reject item')

def __output_tuple_fun(tuple_type):
    """Return a function to output an unnamed or named tuple."""

    if tuple_type is tuple:
        def output_fun(object, count):
            """Output a normal, unnamed tuple."""
            return (object, count)
    else:
        def output_fun(object, count):
            """Output a specified named tuple."""
            return tuple_type(object, count)

    return output_fun

def consume(iterator, n=None):
    "Advance the iterator n-steps ahead. If n is none, consume entirely."
    # Use functions that consume iterators at C speed.
    if n is None:
        # feed the entire iterator into a zero-length deque
        collections.deque(iterator, maxlen=0)
    else:
        # advance to the empty slice starting at position n
        next(islice(iterator, n, n), None)

def integrate_counts(iterator, tuple_type=tuple):
    """
    Given an iterator over (object, count) tuples generate an iterator over
    (object, total count) tuples created by summing the counts of identical
    objects. Only identical objects occurring next to each other are clustered.
    """

    output_fun = __output_tuple_fun(tuple_type)

    current_object = None
    current_count = None

    for item in iterator:
        if item[0] == current_object:
            current_count += item[1]
        else:
            if current_object is not None:
                yield output_fun(current_object, current_count)
            (current_object, current_count) = item

    if current_object is not None:
        yield output_fun(current_object, current_count)

def maximise_counts(iterator1, iterator2, tuple_type=tuple):
    """
    Given two iterators over sorted (object, count) tuples generate an iterator
    over sorted (object, max(count1, count2)) tuples. I.e. for each object
    return the larger of its counts found in the two iterators.

    The objects must be unique in each of the iterators. If an object is
    entirely missing from one of the iterators, it is still returned with a
    count from the iterator where it exists.

    For performance reasons, the second iterator should be the "denser" one,
    i.e. more frequently contain objects that are not in the other iterator.
    """

    output_fun = __output_tuple_fun(tuple_type)

    buffer = tuple()

    for item1 in iterator1:
        for item2 in chain(buffer, iterator2):
            if item2[0] < item1[0]:
                yield item2
                continue
            elif item2[0] == item1[0]:
                yield output_fun(item1[0], max(item1[1], item2[1]))
                break
            else:
                buffer = iter((item2,))
                yield item1
                break

def reject(iterator, rejects):
    """
    Return an iterator over (True/False, (object, *)) tuples. iterator contains
    (object, *) tuples, rejects contains just the object items. True/False
    will indicate whether object from iterator is in rejects. Both iterators
    need to be sorted and unique by object.
    """

    buffer = tuple()

    for reject in rejects:
        for item in chain(buffer, iterator):
            if item[0] < reject:
                yield FilteredTuple(False, item)
                continue
            elif item[0] == reject:
                yield FilteredTuple(True, item)
                break
            else:
                buffer = iter((item,))
                break

    for item in iterator:
        yield FilteredTuple(False, item)
