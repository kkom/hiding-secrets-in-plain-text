def topological_string_sort(lst_unsorted):
    """Sorts a list of strings so that any prefix of a string comes after it."""
    
    def is_prefix(a,b):
        """Returns whether string a is a prefix of string b."""
        return len(a) <= len(b) and b[:len(a)] == a
    
    def visit(n):
        """
        First goes through all the other nodes to look for nodes of which n is a
        prefix. If any such node is found, it is visited recursively. Finally,
        after all topologically preceding nodes are visited, node n is added to
        the sorted list. 
        """
        unseen.remove(n)
        
        # Iterate over all nodes and for the ones that haven't been seen yet
        # check if they should be placed before n. Note that there is no need to
        # discard comparisons with n itself, as it was "seen" in the line above.
        for m in range(len(lst)):
            if m in unseen and is_prefix(lst[n],lst[m]):
                visit(m)
        
        lst_sorted.append(lst[n])
    
    lst = tuple(lst_unsorted)
    lst_sorted = []
    unseen = set(range(len(lst)))
    
    while len(unseen) > 0:
        # The following two lines of code are here only because there is no
        # efficient method to query for an element of a set without popping it
        n = unseen.pop()
        unseen.add(n)
        visit(n)
        
    return lst_sorted
