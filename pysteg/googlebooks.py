import functools

from pysteg.common.graphs import topological_string_sort

@functools.lru_cache(maxsize=512)
def get_partition(prefix, partitions):
    """
    Give the partition which a particular prefix belongs to. Each prefix is
    assigned to a partition that is its maximal prefix.
    """
    
    @functools.lru_cache()
    def order_partitions(partitions):
        return topological_string_sort(partitions)
    
    for partition in order_partitions(partitions):
        if prefix[0:len(partition)] == partition:
            return partition
