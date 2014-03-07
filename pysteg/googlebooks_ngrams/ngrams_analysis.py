import json

from pysteg.googlebooks2 import pos_tagged

def extract_ngram_counts(source_ngrams, n):
    """
    Given an iterator over the lines of a Google Books Ngram corpus file returns
    an iterator over the counts of particular ngrams. The counts are integrated
    over all years and ngrams containing syntactic annotations are discarded.
    
    The function assumes that all lines which have the same ngrams and differ
    only by the year of publication will be adjacent in the file. This
    assumption is needed to generate correct output and is critical to the
    function's performance.
    """
    
    def valid_ngram(ngram):
        """Check if the current ngram is valid, i.e. POS tag-free."""
        for word in ngram:
            if pos_tagged(word):
                return False
        return True
    
    current_ngram = []
    current_ngram_count = 0
    current_ngram_valid = False
    
    for line in source_ngrams:
        data = line.split()
        
        if data[0:n] == current_ngram:
            current_ngram_count += int(data[n+1])
        else:
            if current_ngram_valid:
                yield (tuple(current_ngram), current_ngram_count)
            
            current_ngram = data[0:n]
            current_ngram_count = int(data[n+1])
            current_ngram_valid = valid_ngram(current_ngram)

def gen_ngram_descriptions(filename):
    """Yields ngram descriptions from a file."""
    
    with open(filename, 'r') as f:
        ngrams = json.load(f)
        
    for n in sorted(ngrams.keys()):
        for prefix in ngrams[n]:
            yield (n, prefix)
            
def ngram_filename(n,prefix):
    """Returns a standard ngram filename from its order and prefix."""
    
    return "googlebooks-eng-us-all-{n}gram-20120701-{prefix}".format(**locals())
