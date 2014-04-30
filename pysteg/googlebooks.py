import functools
import itertools

from sympy import ceiling, floor, Rational
import psycopg2

from pysteg.common.db import get_table_name
from pysteg.common.graphs import topological_string_sort

@functools.lru_cache(maxsize=512)
def get_partition(prefix, partitions):
    """
    Give the partition which a particular prefix belongs to. Each prefix is
    assigned to a partition that is its maximal prefix.
    """

    @functools.lru_cache()
    def n_partitions(partitions):
        return topological_string_sort(partitions)

    for partition in n_partitions(partitions):
        if prefix[0:len(partition)] == partition:
            return partition

class GooglebooksNgramsLanguageModel:
    """
    Language model based on ngram frequencies for the Google Books project.
    """

    def __init__(self, database, dataset, n):
        self.database = database
        self.dataset = dataset
        self.n = n

        self.conn = psycopg2.connect(database=self.database)
        self.cur = self.conn.cursor()

    def __del__(self):
        self.conn.close()

    def match_words(self, words):
        """Generate part of an SQL query matching words w1, w2, ..."""
        if len(words) > 0:
            return "WHERE " + " AND ".join(map(
                lambda i, w: "w{i} = {w}".format(**locals()),
                itertools.count(1),
                words
            ))
        else:
            return ""

    def get_row_by_words(self, context, w=None):
        n = len(context) + 1

        if w is None:
            table = get_table_name(self.dataset,
                "{n}grams__context".format(**locals()))
            words = context
        else:
            table = get_table_name(self.dataset, "{n}grams".format(**locals()))
            words = context + (w,)

        where = self.match_words(words)

        self.cur.execute("""
          SELECT
            cf1, cf2
          FROM
            {table}
          {where}
          LIMIT 1;
          """.format(**locals())
        )

        return self.cur.fetchone()

    def c(self, w, context):
        """
        Return a tuple (c1,c2) with the pre- and post- cumulative probabilities
        of a word given the preceding words.
        """
        # Make sure that context does not increase the order of the model
        return self._c_raw(w, context[-(self.n-1):])

    @functools.lru_cache(maxsize=1024)
    def _c_raw(self, w, context):
        n = len(context) + 1

        # Check if the ngram [context + (w)] exists in the database
        ngram_row = self.get_row_by_words(context, w)

        if ngram_row is None:
            # Back off to a lower-order model or raise an exception
            if n > 1:
                return self._c_raw(w, context[1:])
            else:
                raise Exception("Word {w} not in alphabet.".format(**locals()))
        else:
            # Calculate the cumulative probabilities. Explanation of variables:
            #
            # cf - cumulative frequencies of the ngram [context + (w)]
            # CF - cumulative frequencies of the ngram [context]
            # c  - cumulative probabilities of w with the given context

            context_row = self.get_row_by_words(context)
            cf1, cf2 = ngram_row
            CF1, CF2 = context_row
            c1 = Rational(cf1 - CF1, CF2 - CF1)
            c2 = Rational(cf2 - CF1, CF2 - CF1)

            return (c1,c2)

    def next(self, c, context):
        """
        Give the next word given its context and probability subinterval it has
        to contain.
        """
        # Make sure that context does not increase the order of the model
        return self._next_raw(c, context[-(self.n-1):])

    @functools.lru_cache(maxsize=1024)
    def _next_raw(self, c, context):
        n = len(context) + 1

        # Check if the context can be found for this order model and what are
        # its cumulative frequencies
        context_row = self.get_row_by_words(context)

        if context_row is None:
            # Back off to a lower-order model or raise an exception
            if n > 1:
                return self._next_raw(c, context[1:])
            else:
                raise Exception("1-grams context table broken.")
        else:
            # Calculate the cumulative probabilities interval the next word must
            # contain. Explanation of variables:
            #
            # cf - cumulative frequencies of the ngram [context + (w)]
            # CF - cumulative frequencies of the ngram [context]
            # c  - cumulative probabilities of w with the given context

            c1, c2 = c
            CF1, CF2 = context_row
            cf1 = floor(CF1 + c1 * (CF2 - CF1))
            cf2 = ceiling(CF1 + c2 * (CF2 - CF1))

            table = get_table_name(self.dataset, "{n}grams".format(**locals()))

            self.cur.execute("""
              SELECT
                w{n}
              FROM
                {table}
              WHERE
                cf1 <= {cf1} AND cf2 >= {cf2}
              LIMIT 1;
              """.format(**locals())
            )

            ngram_row = self.cur.fetchone()

            if ngram_row is None:
                return None
            else:
                return ngram_row[0]
