import re

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
    
    pos_tags = {"NOUN", "VERB", "ADJ", "ADV", "PRON", "DET", "ADP", "NUM",
        "CONJ", "PRT", ".", "X"}
    
    # A part-of-speech (POS) tag can either be appended to the end of a word, as
    # in "eat_VERB", or can be a placeholder on its own, as in "_VERB_".
    pos_pattern = re.compile("_[A-Z.]+_?$")
    
    def valid_ngram(ngram):
        """Check if the current ngram is valid, i.e. POS tag-free."""
        
        for word in ngram:
            for potential_tag in pos_pattern.findall(word.decode()):
                if potential_tag.strip("_") in pos_tags:
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
