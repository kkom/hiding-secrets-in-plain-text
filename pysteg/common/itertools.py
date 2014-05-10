import collections

from itertools import chain, islice

def consume(iterator, n=None):
    "Advance the iterator n-steps ahead. If n is none, consume entirely."
    # Use functions that consume iterators at C speed.
    if n is None:
        # feed the entire iterator into a zero-length deque
        collections.deque(iterator, maxlen=0)
    else:
        # advance to the empty slice starting at position n
        next(islice(iterator, n, n), None)

def integrate_counts(iterator):
    """
    Given an iterator over (object, count) tuples generate an iterator over
    (object, total count) tuples created by summing the counts of identical
    objects. Only identical objects occurring next to each other are clustered.
    """

    current_object = None
    current_count = None

    for item in iterator:
        if item[0] == current_object:
            current_count += item[1]
        else:
            if current_object is not None:
                yield (current_object, current_count)
            (current_object, current_count) = item

    if current_object is not None:
        yield (current_object, current_count)

def maximise_counts(iterator1, iterator2):
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

    buffer = tuple()

    for item1 in iterator1:
        for item2 in chain(buffer, iterator2):
            if item2[0] < item1[0]:
                yield item2
                continue
            elif item2[0] == item1[0]:
                yield (item1[0], max(item1[1], item2[1]))
                break
            else:
                buffer = iter((item2,))
                yield item1
                break
