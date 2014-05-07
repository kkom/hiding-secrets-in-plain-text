import collections
import functools
import struct

BinDBLine = collections.namedtuple('BinDBLine', 'ngram count')

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

@functools.lru_cache(maxsize=8)
def fmt(n):
    """Format specifier for a BinDBLine of order n."""
    return (
        "<" +     # little-endian byte order (native for x86 and x86-64 CPUs)
        n * "i" + # n * 4 byte integers with token indices
        "q"       # 8 byte integer with ngram count
    )

def iter_bindb_file(f, n):
    """
    Iterate over the lines of a BinDB file. Lines are given in BinDBLine format.
    """

    bindb_line = f.read(line_size(n))
    while len(bindb_line) != 0:
        yield unpack_line(bindb_line, n)
        bindb_line = f.read(line_size(n))

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
