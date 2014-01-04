import gzip
import itertools
import urllib.request

def iter_remote_gzip(url):
    """Returns an iterator over the lines of a remote gzipped text file."""

    with urllib.request.urlopen(url) as f:
        with gzip.GzipFile(fileobj=f) as g:
            for l in g:
                yield l
