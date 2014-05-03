def read_index_t2ip(f):
    """
    Return a dictionary keyed by tokens and valued by (index, partition) tuples.
    """

    def read_index_line_t2ip(line):
        """
        Given an index file line returns a (token, (index, partition)) tuple.
        """
        line_split = line[:-1].split("\t")
        return (line_split[1], (int(line_split[0]), line_split[2]))

    return dict(map(read_index_line_t2ip, f))
