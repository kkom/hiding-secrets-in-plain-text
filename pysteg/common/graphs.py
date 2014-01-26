def topological_string_sort(l_unsorted):
    """Sorts a list of strings so that any prefix of a string comes after it."""
    
    def visit(n):
        unseen.remove(n)
        
        for m in range(len(l)):
            if n != m and l[n] in l[m] and m in unseen:
                visit(m)
        
        l_sorted.append(l[n])
    
    l = tuple(l_unsorted)
    l_sorted = []
    unseen = set(range(len(l)))
    
    while len(unseen) > 0:
        # The following two lines of code are here only because there is no
        # efficient method to query for an element of a set without popping it
        n = unseen.pop()
        unseen.add(n)
        visit(n)
        
    return l_sorted
