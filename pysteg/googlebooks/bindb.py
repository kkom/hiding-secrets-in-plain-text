import collections
import functools
import itertools
import math
import os
import struct
import sympy

from pysteg.coding.rational_ac import create_interval, select_subinterval

from pysteg.common.itertools import reject

BinDBLine = collections.namedtuple('BinDBLine', 'ngram count')
TokenCount = collections.namedtuple('TokenCount', 'token count')

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

    def bs(self, n, mgram, imin=1, imax=None, mode="low", ratio=0.5):
        """
        Binary search for the first or last ngram with first m tokens equal to
        the given mgram. The ratio parameter specifies where the midpoint
        between imin and imax should be located.
        """

        assert(mode in ("low", "high"))

        m = len(mgram)

        # mgram size cannot be larger than the order of the searched table
        assert(m <= n)

        def get_ngram(i):
            """
            Return i'th ngram from the specified table truncated to the size of
            the searched mgram.
            """
            return read_line(self.f[n], n, i).ngram[:m]

        if imax == None:
            imax = self.size[n]

        while imin < imax:
            if mode == "low":
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

    def range_search(self, n, mgram):
        """
        Search for the range of ngrams with first m tokens equal to the given
        mgram.
        """

        if len(mgram) == 0:
            return (1, self.size[n])

        low_i = self.bs(n, mgram, mode="low")
        if low_i is not None:
            high_i = self.bs(n, mgram, imin=low_i, mode="high", ratio=0.1)
            return (low_i, high_i)
        else:
            return None

    def conditional_interval(self, token, context):
        """Return the conditional probability interval of a token."""

        # Only use context within the order of the model
        context = context[-(self.n_max-1):]

        return self._raw_conditional_interval(token, context, None)

    def _iter_matching_tokens(self, context, backed_off):
        """
        Yield BinDB lines matching a particular context of length (n-1),
        optionally excluding ngrams which would be covered by a higher order
        model.
        """

        # At the beginning of a sentence or directly after an _END_ token, the
        # only option is a _START_ token.
        if ((len(context) == 0 and backed_off is None) or
            (len(context) > 0 and context[-1] == self.end)):
            yield TokenCount(self.start, 1)
            return

        # Find ngrams matching the context
        n = len(context) + 1
        ngrams_range = self.range_search(n, context)

        # If there are no matching ngrams, back-off is the only option
        if ngrams_range is None:
            yield TokenCount(self.backoff, 1)
            return

        # Make an iterator of ngrams matching the context
        (low_i, high_i) = ngrams_range
        ngrams = iter_bindb_file(self.f[n], n, low_i, high_i-low_i+1)

        if backed_off is None:
            # If we didn't back-off, do not reject any ngrams
            rejects = ()
        else:
            # If we backed-off from a higher order context, do not consider the
            # ngrams which were already covered by the higher order model
            (low_i, high_i) = self.range_search(n+1, (backed_off,) + context)
            ograms = iter_bindb_file(self.f[n+1], n+1, low_i, high_i-low_i+1)

            # Ngrams which should not be considered in this level of the
            # conditional probability tree
            rejects = map(lambda l: l.ngram[1:], ograms)

        filtered_ngrams = reject(ngrams, rejects)

        # Yield accepted tokens and find cumulative counts needed for
        # calculating the back-off weight
        total_accepted_count = 0
        total_rejected_count = 0
        for i in filtered_ngrams:
            # Always reject the case when the next token is _START_ - there are
            # special rules for this token covered in the beginning
            if i.reject or i.item.ngram[-1] == self.start:
                total_rejected_count += i.item.count
            else:
                total_accepted_count += i.item.count
                yield TokenCount(i.item.ngram[-1], i.item.count)

        # Calculate the back-off pseudo-count
        if n > 1:
            total_context_count = read_line(
                self.f[n-1], n-1, self.bs(n-1, context)
            ).count
            context_count = total_context_count - total_rejected_count
            leftover_probability_mass = context_count - total_accepted_count
            backoff_pseudocount = math.ceil(
                self.alpha * leftover_probability_mass
                + self.beta * context_count
            )
            yield TokenCount(self.backoff, backoff_pseudocount)

    @functools.lru_cache(maxsize=512)
    def _raw_conditional_interval(self, token, context, backed_off):
        """"Internal version of the conditional probability interval method."""

        # _START_ token has a 100% probability of occurring at the beginning of
        # a sentence (empty context and not backed-off) or after the _END_
        # token. All other occurrences are a mistake.
        if token == self.start:
            if ((len(context) == 0 and backed_off is None) or
                (len(context) > 0 and context[-1] == self.end)):
                return create_interval(0,1)
            else:
                raise Exception("_START_ token in an incorrect position.")

        n = len(context) + 1

        # Find ngrams matching the context
        ngrams_range = self.range_search(n, context)

        # If there are no matching ngrams, the back-off pseudo-count is going to
        # be 100% of the probability mass, so we can directly report the
        # backed-off conditional probability. Since there are no matching ngrams
        # in this level of the tree, the lower-order conditional probability is
        # calculated as if it wasn't backed-off (consider all possible ngrams).
        if ngrams_range is None:
            return self._raw_conditional_interval(token, context[1:], None)

        # Make an iterator of ngrams matching the context
        (low_i, high_i) = ngrams_range
        ngrams = iter_bindb_file(self.f[n], n, low_i, high_i-low_i+1)

        if backed_off is None:
            # If we didn't back-off, do not reject any ngrams
            rejects = ()
        else:
            # If we backed-off from a higher order context, do not consider the
            # ngrams which were already covered by the higher order model
            (low_i, high_i) = self.range_search(n+1, (backed_off,) + context)
            ograms = iter_bindb_file(self.f[n+1], n+1, low_i, high_i-low_i+1)

            # Ngrams which should not be considered in this level of the
            # conditional probability tree
            rejects = map(lambda l: l.ngram[1:], ograms)

        filtered_ngrams = reject(ngrams, rejects)

        # Find all cumulative counts needed to calculate the conditional
        # probability interval
        token_interval_counts = None
        total_accepted_count = 0
        total_rejected_count = 0
        for i in filtered_ngrams:
            # Always reject the unigram _START_ - it cannot be freely chosen
            if i.reject or (n == 1 and i.item.ngram[0] == self.start):
                total_rejected_count += i.item.count
            else:
                if i.item.ngram[-1] == token:
                    token_interval_counts = (total_accepted_count, i.item.count)
                total_accepted_count += i.item.count

#         print("total_accepted_count: " + str(total_accepted_count))
#         print("total_rejected_count: " + str(total_rejected_count))
#         print("token_interval_counts: " + str(token_interval_counts))

        # Calculate the back-off pseudo-count
        if n == 1:
            backoff_pseudocount = 0
        else:
            total_context_count = read_line(
                self.f[n-1], n-1, self.bs(n-1, context)
            ).count
            context_count = total_context_count - total_rejected_count
            leftover_probability_mass = context_count - total_accepted_count
            backoff_pseudocount = math.ceil(
                self.alpha * leftover_probability_mass
                + self.beta * context_count
            )

#             print("total_context_count: " + str(total_context_count))
#             print("context_count: " + str(context_count))
#             print("leftover_probability_mass: " + str(leftover_probability_mass))
#             print("backoff_pseudocount: " + str(backoff_pseudocount))

        all_counts = total_accepted_count + backoff_pseudocount

        if token_interval_counts:
            # If the token was found in the ngrams, report its interval
            return create_interval(token_interval_counts[0],
                                   token_interval_counts[1], all_counts)
        else:
            # Otherwise, back-off the model and report the backed-off interval
            # within the probability mass assigned for back-off
            backoff_interval = create_interval(total_accepted_count, all_counts,
                                               all_counts)
            backoff_subinterval = self._raw_conditional_interval(
                token, context[1:], context[0]
            )

            return select_subinterval(backoff_interval, backoff_subinterval)

@functools.lru_cache(maxsize=8)
def fmt(n):
    """Format specifier for a BinDBLine of order n."""
    return (
        "<" +     # little-endian byte order (native for x86 and x86-64 CPUs)
        n * "i" + # n * 4 byte integers with token indices
        "q"       # 8 byte integer with ngram count
    )

def iter_bindb_file(f, n, start=1, number_iters=float("Inf")):
    """
    Iterate over the lines of a BinDB file. Lines are given in BinDBLine format.
    """

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
