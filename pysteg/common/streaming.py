import gzip
import itertools
import urllib.request

def iter_remote_gzip(url):
    """Returns an iterator over the lines of a remote gzipped text file."""

    with urllib.request.urlopen(url) as f:
        with gzip.GzipFile(fileobj=f) as g:
            for l in g:
                yield l

def ngrams_iter2file(ngrams, f):
    """
    Writes an iterable over ngrams into a file. The ngrams have to be in the
    standard (ngrams_iterable, count) format and the words saved as Bytes. The
    file has to be open in binary mode.
    """

    for ngram in ngrams:
        for word in ngram[0]:
            f.write(word)
            f.write(b'\t')

        f.write(bytes(str(ngram[1]), "utf-8"))
        f.write(b'\n')
