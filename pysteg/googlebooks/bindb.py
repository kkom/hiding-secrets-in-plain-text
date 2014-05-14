import collections
import functools
import itertools
import math
import os
import struct
import sympy

from pysteg.common.itertools import reject

from pysteg.coding.interval import create_interval
from pysteg.coding.interval import find_ratio
from pysteg.coding.interval import select_subinterval

BinDBLine = collections.namedtuple('BinDBLine', 'ngram count')
TokenCount = collections.namedtuple('TokenCount', 'token b l')

class BinDBIndex:
    """
    Index of a BinDB database. Provides fast methods to go between indices and
    string of tokens.
    """

    def __init__(self, f):
        """
        Initialise the index. The index file is a text file that for each token
        has a single line containing 3 tab separated values:

        1. index of the token (from 1 to vocabulary size V)
        2. string corresponding to the token
        3. partition of the token

        The index file has to be already sorted by the first column.
        """

        self.index_dict = {}
        index_list = []

        for l in f:
            l_split = l[:-1].split("\t")
            self.index_dict[l_split[1]] = (int(l_split[0]), l_split[2])
            index_list.append(l_split[1])

        self.index_tuple = tuple(index_list)

    def i2s(self, i):
        """Return the string of a token given its index."""
        return self.index_tuple[i-1]

    def s2i(self, t):
        """Return the index of a token given its string."""
        return self.index_dict[t][0]

    def s2p(self, t):
        """Return the partition of a token given its string."""
        return self.index_dict[t][1]

class BinDBLM:
    """
    A BinDB-based language model. Gives conditional probability intervals and
    find the next token given the history and probability intervals it needs to
    be around.
    """

    def __init__(self, bindb_dir, n_max, start, end, alpha, beta):
        self.n_max = n_max          # Order of the model
        self.start = start          # Indices of the _START_ and _END_ tokens
        self.end = end
        self.alpha = alpha          # Proportion of leftover ML probability mass
                                    # assigned to the back-off path
        self.beta = beta            # Extra probability mass assigned to the
                                    # back-off path

        paths = dict((n, os.path.join(bindb_dir, "{n}gram".format(**locals())))
                     for n in range(1,n_max+1))

        self.f = dict((n, open(path, "rb")) for n, path in paths.items())

        self.size = dict((n, int(os.path.getsize(path)/line_size(n)))
                         for n, path in paths.items())

        # A pseudo-token for back-off
        self.backoff = self.size[1] + 1

    def __del__(self):
        for f in self.f.values():
            f.close()

    def _bs(self, n, mgram, imin=1, imax=None, mode="first", ratio=0.5):
        """
        Binary search for the first or last ngram with first m tokens equal to
        the given mgram. The ratio parameter specifies where the midpoint
        between imin and imax should be located.
        """

        assert(mode in ("first", "last"))

        # mgram size cannot be larger than the order of the searched table
        m = len(mgram)
        assert(m <= n)

        def get_ngram(i):
            """
            Return from the specified table i'th ngram truncated to the size of
            the searched mgram.
            """
            return read_line(self.f[n], n, i).ngram[:m]

        if imax == None:
            imax = self.size[n]

        # Binary search loop with deferred detection of equality to find the
        # first or last match
        while imin < imax:
            if mode == "first":
                imid = math.floor(imin+ratio*(imax-imin))
                if get_ngram(imid) < mgram:
                    imin = imid + 1
                else:
                    imax = imid
            else:
                imid = math.ceil(imin+ratio*(imax-imin))
                if get_ngram(imid) > mgram:
                    imax = imid - 1
                else:
                    imin = imid

        if get_ngram(imin) == mgram:
            return imin
        else:
            return None

    def _bs_range(self, n, mgram):
        """
        Binary search for the range of ngrams with first m tokens equal to the
        given mgram.
        """

        # If the mgram is empty, every ngram in the table matches it
        if len(mgram) == 0:
            return (1, self.size[n])

        ifirst = self._bs(n, mgram, mode="first")

        if ifirst is not None:
            # At least one ngram matches the mgram
            ilast = self._bs(n, mgram, imin=ifirst, mode="last", ratio=0.1)
            return (ifirst, ilast)
        else:
            return None

    def conditional_interval(self, token, context):
        """Return the conditional probability interval of a token."""

        # Only use context within the order of the model
        context = context[-(self.n_max-1):]

        return self._raw_conditional_interval(token, context, None)

    def next(self, interval, context):
        """Return the next token given current context and interval."""

        # Only use context within the order of the model
        context = context[-(self.n_max-1):]

        return self._raw_next(interval, context, None)

    def _iter_matching_tokens(self, context, backed_off):
        """
        Iterate over BinDB lines corresponding to ngrams matching a particular
        context of length (n-1), optionally excluding ngrams which would be
        covered by a model one order higher.
        """

        # At the beginning of a sentence or directly after an _END_ token, the
        # only option is a _START_ token.
        if ((len(context) == 0 and backed_off is None) or
            (len(context) > 0 and context[-1] == self.end)):
            yield TokenCount(self.start, 0, 1)
            return

        # Find ngrams matching the context
        n = len(context) + 1
        ngrams_range = self._bs_range(n, context)

        # If there are no matching ngrams, back-off is the only option
        if ngrams_range is None:
            yield TokenCount(self.backoff, 0, 1)
            return

        # If the order of the table to iterate is 1, it should be done from the
        # cache. It has a few million entries and each time it is iterated over
        # every entry needs to be read.
        cache = n==1

        # Make an iterator of ngrams matching the context
        (ifirst, ilast) = ngrams_range
        ngrams = iter_bindb_file(self.f[n], n, ifirst, ilast-ifirst+1, cache)

        if backed_off is None:
            # If we didn't back-off, do not reject any ngrams
            rejects = ()
        else:
            # If we backed-off from a higher order context, do not consider the
            # ngrams which were already covered by the higher order model
            ograms_range = self._bs_range(n+1, (backed_off,) + context)

            if ograms_range is None:
                # There are no matching higher order tokens, so no rejects
                rejects = ()
            else:
                # Reject tokens which would be matched by a higher order model
                (ifirst, ilast) = ograms_range
                ograms = iter_bindb_file(self.f[n+1], n+1, ifirst,
                                         ilast-ifirst+1)

                # Ngrams which should not be considered in this level of the
                # conditional probability tree
                rejects = map(lambda l: l.ngram[1:], ograms)

        filtered_ngrams = reject(ngrams, rejects)

        # Yield accepted tokens and find cumulative counts needed for
        # calculating the back-off weight
        total_accepted_count = 0
        total_rejected_count = 0
        for i in filtered_ngrams:
            # Always reject the case when the last token is _START_. In practice
            # this will only happen when considering unigrams. The reason for it
            # is that _START_ is only possible in certain situations, which are
            # covered in the beginning.
            if i.reject or i.item.ngram[-1] == self.start:
                total_rejected_count += i.item.count
            else:
                yield TokenCount(i.item.ngram[-1],
                                 total_accepted_count, i.item.count)
                total_accepted_count += i.item.count

        # Calculate the back-off pseudo-count
        if n > 1:
            total_context_count = read_line(
                self.f[n-1], n-1, self._bs(n-1, context)
            ).count
            context_count = total_context_count - total_rejected_count

            # If all context was already explored, back-off is the only option
            if context_count == 0:
                yield TokenCount(self.backoff, 0, 1)
                return

            leftover_probability_mass = context_count - total_accepted_count
            backoff_pseudocount = math.ceil(
                self.alpha * leftover_probability_mass
                + self.beta * context_count
            )
            yield TokenCount(self.backoff,
                             total_accepted_count, backoff_pseudocount)

    @functools.lru_cache(maxsize=512)
    def _raw_conditional_interval(self, token, context, backed_off):
        """Internal version of the conditional probability interval method."""

        match = None
        backoff_token = None

        for i in self._iter_matching_tokens(context, backed_off):
            if i.token == token:
                match = i
            if i.token == self.backoff:
                backoff_token = i
            full_count = i.b + i.l

        if match is not None:
            # If the token was found in the ngrams, report its interval
            return create_interval(match.b, match.l, full_count)
        elif backoff_token is not None:
            # Otherwise, back-off the model and report the backed-off interval
            # within the probability mass assigned to back-off
            backoff_interval = create_interval(
                backoff_token.b, backoff_token.l, full_count)
            backoff_subinterval = self._raw_conditional_interval(
                token, context[1:], context[0]
            )
            return select_subinterval(backoff_interval, backoff_subinterval)
        else:
            raise Exception('Impossible sentence.')

    @functools.lru_cache(maxsize=512)
    def _raw_next(self, search_interval, context, backed_off):
        """Internal version of the next token method."""

        tokens = tuple(self._iter_matching_tokens(context, backed_off))

        # Find correct scaled interval
        full_count = tokens[-1].b + tokens[-1].l
        base = sympy.floor(search_interval.b * full_count)
        end = sympy.ceiling((search_interval.b+search_interval.l) * full_count)
        length = end - base

        def interval_bs(tokens, base, end):
            """
            Find using binary search a token whose counts are a superinterval of
            [base, end].
            """
            imin = 0
            imax = len(tokens)-1

            while imin <= imax:
                imid = round((imin+imax)/2)

                if (tokens[imid].b <= base and
                    tokens[imid].b + tokens[imid].l >= base + length):
                    return imid
                elif tokens[imid].b + tokens[imid].l <= base:
                    imin = imid + 1
                else:
                    imax = imid - 1

        i = interval_bs(tokens, base, end)

        if i is None:
            # No token can be found
            return None
        else:
            # We have found a token -- standard or back-off
            token = tokens[i]
            token_interval = create_interval(token.b, token.l, full_count)
            scaled_search_interval = find_ratio(search_interval, token_interval)

            if token.token == self.backoff:
                return self._raw_next(scaled_search_interval, context[1:],
                                      context[0])

            else:
                return (token.token, scaled_search_interval)

@functools.lru_cache(maxsize=8)
def fmt(n):
    """Format specifier for a BinDBLine of order n."""
    return (
        "<" +     # little-endian byte order (native for x86 and x86-64 CPUs)
        n * "i" + # n * 4 byte integers with token indices
        "q"       # 8 byte integer with ngram count
    )

_iter_bindb_file_cache = dict()

def iter_bindb_file(f, n, start=1, number_iters=float("Inf"), cache=False):
    """
    Iterate over the lines of a BinDB file. Lines are given in BinDBLine format.

    The result can be cached, but note that the cache is indexed only based on
    the order of the table. So contents of the first cached file of order n will
    always be returned, regardless of the path to the actual file.
    """

    if cache:
        # Put the file in cache, if not yet in there
        if n not in _iter_bindb_file_cache:
            _iter_bindb_file_cache[n] = tuple(iter_bindb_file(f,n,cache=False))

        # Yield results from the cached table
        cached_table = _iter_bindb_file_cache[n]
        for i in range(start-1, min(start+number_iters-1, len(cached_table))):
            yield cached_table[i]
        return

    else:
        # Go to the start line
        f.seek((start-1)*line_size(n))

        # Read the first line in bytes
        i = 1
        bindb_line = f.read(line_size(n))

        while len(bindb_line) != 0 and i <= number_iters:
            yield unpack_line(bindb_line, n)
            bindb_line = f.read(line_size(n))
            i += 1

def line_size(n):
    """Return the size in bytes of a BinDBLine of order n."""
    # 4 bytes for each word index and 8 bytes for the count
    return 4*n+8

def pack_line(bindb_line, n):
    """Pack a BinDB line of order n into bytes."""
    return struct.pack(fmt(n), *bindb_line.ngram+(bindb_line.count,))

def read_line(f, n, i):
    """
    Return the i'th (1-indexed) line of a BinDB file as a BinDBLine.
    """

    # Go to the i'th line
    f.seek((i-1)*line_size(n))

    return unpack_line(f.read(line_size(n)), n)

def unpack_line(line_bytes, n):
    """Unpack a BinDBLine of order n from bytes."""
    line = struct.unpack(fmt(n), line_bytes)
    return BinDBLine(line[:-1], line[-1])
