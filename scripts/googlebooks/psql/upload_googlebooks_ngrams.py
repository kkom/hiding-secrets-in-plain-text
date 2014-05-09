#!/usr/bin/env python3

descr = """
This scripts upload Google Books Ngrams to a local PostgreSQL database.
"""

import argparse
import json
import os

import psycopg2

from pysteg.common.db import get_table_name
from pysteg.googlebooks.psql import get_partition
from pysteg.googlebooks.ngrams_analysis import ngram_filename

def partition_ngrams(ngram_descriptions):
    """Returns ngram descriptions dictionary with the prefixes partitioned."""

    partitions_set = frozenset(ngram_descriptions["1"])
    ngrams = {}

    for n in ngram_descriptions.keys():
        ngrams[n] = {}
        for prefix in ngram_descriptions[n]:
            partition = get_partition(prefix, partitions_set)
            if partition not in ngrams[n]:
                ngrams[n][partition] = [prefix]
            else:
                ngrams[n][partition].append(prefix)

    return ngrams

def gen_job_descriptions(partitioned_ngrams, cumfreq_ranges):
    for n in sorted(partitioned_ngrams.keys()):
        yield (int(n), partitioned_ngrams[n], index_ranges, cumfreq_ranges[n])

def complete(task):
    """Record in the filesystem that a task has been completed."""
    open(os.path.join(args.progress_dir, task), "w").close()

def is_completed(task):
    """Check if a task has been completed."""
    return os.path.isfile(os.path.join(args.progress_dir, task))

def upload_ngrams(n, prefixes, index_ranges, cumfreq_ranges):
    """Upload ngrams for a particular n to the PostgreSQL database."""

    def get_column_definitions(n):
        return ",\n".join(map(
            lambda x: "w{} INTEGER".format(x),
            range(1, n+1)
        ))

    def get_column_names(n):
        return ", ".join(map(lambda x: "w{}".format(x), range(1, n+1)))

    # Generate table and columns definitions
    table = get_table_name(args.dataset, "{n}grams".format(**locals()))
    context_table = get_table_name(args.dataset,
        "{n}grams__context".format(**locals()))
    column_definitions = get_column_definitions(n)
    columns = get_column_names(n)

    if not is_completed("{n}grams_create_parent_tables".format(**locals())):
        # Create parent ngrams table
        cur.execute("""
            DROP TABLE IF EXISTS {table} CASCADE;

            CREATE TABLE {table} (
              i SERIAL,
              {column_definitions},
              cf1 BIGINT,
              cf2 BIGINT
            );
            """.format(**locals())
        )
        print("Created TABLE {table}".format(**locals()))

        # Create parent context table
        if n > 1:
            context_column_definitions = get_column_definitions(n-1)
            context_columns = get_column_names(n-1)

            cur.execute("""
                DROP TABLE IF EXISTS {context_table} CASCADE;

                CREATE TABLE {context_table} (
                  i SERIAL,
                  {context_column_definitions},
                  cf1 BIGINT,
                  cf2 BIGINT
                );
                """.format(**locals())
            )
        else:
            cur.execute("""
                DROP TABLE IF EXISTS {context_table};

                CREATE TABLE {context_table} (
                  i SERIAL PRIMARY KEY,
                  cf1 BIGINT,
                  cf2 BIGINT
                );
                """.format(**locals())
            )
        print("Created context TABLE {context_table}".format(**locals()))

        # Commit defining parent tables
        conn.commit()
        complete("{n}grams_create_parent_tables".format(**locals()))

    # Populate respective partition tables
    for partition in sorted(prefixes.keys()):
        if is_completed("{n}grams_{partition}_analyse_partition".format(**locals())):
            continue

        # Define various properties of the partition table, such as its name and
        # the range of data it is supposed to contain
        partition_table = get_table_name(args.dataset,
            "{n}grams_{partition}".format(**locals()))
        index_range = index_ranges[partition]
        cumfreq_range = cumfreq_ranges[partition]

        if not is_completed("{n}grams_{partition}_create_tables".format(**locals())):
            # Create the partition table
            cur.execute("""
                DROP TABLE IF EXISTS {partition_table};

                CREATE TABLE {partition_table} (
                  PRIMARY KEY (i),
                  CHECK (     w1 >= {index_range[0]}
                          AND w1 <= {index_range[1]}
                          AND cf1 >= {cumfreq_range[0]}
                          AND cf1 <= {cumfreq_range[1]}
                          AND cf2 >= {cumfreq_range[0]}
                          AND cf2 <= {cumfreq_range[1]} )
                ) INHERITS ({table});
                """.format(**locals())
            )
            print("Created partition TABLE {partition_table}".format(**locals()))

            # If n > 1, then data in the context table should be partitioned too
            if n > 1:
                context_partition_table = get_table_name(args.dataset,
                    "{n}grams_{partition}__context".format(**locals()))

                cur.execute("""
                    DROP TABLE IF EXISTS {context_partition_table};

                    CREATE TABLE {context_partition_table} (
                      PRIMARY KEY (i),
                      CHECK (     w1 >= {index_range[0]}
                              AND w1 <= {index_range[1]}
                              AND cf1 >= {cumfreq_range[0]}
                              AND cf1 <= {cumfreq_range[1]}
                              AND cf2 >= {cumfreq_range[0]}
                              AND cf2 <= {cumfreq_range[1]} )
                    ) INHERITS ({context_table});
                    """.format(**locals())
                )
                print("Created context partition TABLE "
                      "{context_partition_table}".format(**locals()))

            # Commit creating ngrams and context partition tables
            conn.commit()
            complete("{n}grams_{partition}_create_tables".format(**locals()))

        for prefix in prefixes[partition]:
            if is_completed("{n}grams_{prefix}_analyse_prefix".format(**locals())):
                continue

            path = os.path.join(args.input, ngram_filename(n, prefix))
            raw_tmp_table = get_table_name(args.dataset,
                "tmp_raw__{n}grams_{prefix}".format(**locals()))
            cumfreq_tmp_table = get_table_name(args.dataset,
                "tmp_cumfreq__{n}grams_{prefix}".format(**locals()))

            # Copy ngrams starting with a particular prefix into a temporary
            # table and cumulate their frequencies
            cur.execute("""
                DROP TABLE IF EXISTS {raw_tmp_table};

                CREATE TABLE {raw_tmp_table} (
                  i SERIAL PRIMARY KEY,
                  {column_definitions},
                  f BIGINT
                );

                DROP TABLE IF EXISTS {cumfreq_tmp_table};

                CREATE TABLE {cumfreq_tmp_table} (
                  i SERIAL PRIMARY KEY,
                  {column_definitions},
                  cf1 BIGINT,
                  cf2 BIGINT
                );

                COPY
                  {raw_tmp_table} ({columns}, f)
                FROM
                  %s;

                INSERT INTO
                  {cumfreq_tmp_table} ({columns}, cf1, cf2)
                SELECT
                  {columns},
                  sum(f) OVER (ORDER BY {columns} ASC) - f
                    + (SELECT coalesce(max(cf2),0) FROM {table}) AS cf1,
                  sum(f) OVER (ORDER BY {columns} ASC)
                    + (SELECT coalesce(max(cf2),0) FROM {table}) AS cf2
                FROM
                  {raw_tmp_table};

                DROP TABLE {raw_tmp_table};
                """.format(**locals()),
                (path,)
            )
            print("Copied FILE {path} to TABLE {cumfreq_tmp_table}".format(
                **locals()))

            # Insert ngrams with this prefix into the partition table
            cur.execute("""
                INSERT INTO
                  {partition_table} ({columns}, cf1, cf2)
                SELECT
                  {columns}, cf1, cf2
                FROM
                  {cumfreq_tmp_table}
                ORDER BY
                  i ASC;
                """.format(**locals())
            )
            print("Copied TABLE {cumfreq_tmp_table} to TABLE "
                  "{partition_table}".format(**locals()))

            # Insert ngrams with this prefix into the context partition table
            if n > 1:
                cur.execute("""
                  INSERT INTO
                    {context_partition_table} ({context_columns}, cf1, cf2)
                  SELECT
                    {context_columns},
                    min(cf1) AS cf1,
                    max(cf2) AS cf2
                  FROM
                    {cumfreq_tmp_table}
                  GROUP BY
                    {context_columns}
                  -- This is much faster than "ORDER BY min(i)", can investigate
                  ORDER BY
                    {context_columns} ASC;
                  """.format(**locals())
                )
                print("Cumulated and copied TABLE {cumfreq_tmp_table} to TABLE "
                      "{context_partition_table}".format(**locals()))

            cur.execute("""
              DROP TABLE {cumfreq_tmp_table};
              """.format(**locals())
            )

            # Commit changes due to processing a single prefix file
            conn.commit()
            complete("{n}grams_{prefix}_analyse_prefix".format(**locals()))

        # Index the ngrams partition table. Making the index on columns unique
        # ensures that no leaves of the probability tree are duplicated.
        cur.execute("""
            CREATE UNIQUE INDEX ON {partition_table}
                USING btree ({columns})
                WITH (fillfactor = 100);

            CREATE UNIQUE INDEX ON {partition_table}
                USING btree (cf1, cf2)
                WITH (fillfactor = 100);
            """.format(**locals())
        )
        print("Created UNIQUE INDEXES on ({columns}) and (cf1, cf2) in TABLE "
              "{partition_table}".format(**locals()))

        # Index the ngrams context partition table. Since ngrams are added from
        # the prefix files sequentially, if it happened that two ngrams starting
        # with the same (w1, ..., w(n-1)) were wrongly put in different prefix
        # files, an error will occur. Ngrams starting with the same (w1, ...,
        # w(n-2)) are not a problem, since we will always query for P(w(n) | w1,
        # ..., w(n-1)).
        if n > 1:
            cur.execute("""
              CREATE UNIQUE INDEX ON {context_partition_table}
                  USING btree ({context_columns})
                  WITH (fillfactor = 100);
              """.format(**locals())
            )
            print("Created UNIQUE INDEX on ({context_columns}) in TABLE "
                  "{context_partition_table}".format(**locals()))

        # Commit indexing ngrams and context tables after processing all
        # corresponding prefix files
        conn.commit()
        complete("{n}grams_{partition}_analyse_partition".format(**locals()))

    # Create context for 1 grams
    if n == 1:
        cur.execute("""
          INSERT INTO
            {context_table} (cf1, cf2)
          SELECT
            min(cf1) AS cf1,
            max(cf2) AS cf2
          FROM
            {table};
          """.format(**locals())
        )
        print("Cumulated and copied TABLE {table} to TABLE "
              "{context_table}".format(**locals()))

        # Commit creating context for 1grams
        conn.commit()

    complete("{n}grams_analyse".format(**locals()))

if __name__ == '__main__':
    # Define and parse arguments
    parser = argparse.ArgumentParser(
        description=descr,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("ngrams", help="JSON file listing all the ngram files")
    parser.add_argument("input", help="input directory of ngram files")
    parser.add_argument("index_ranges",
        help="JSON file listing first word index ranges for each partition")
    parser.add_argument("cumfreq_ranges",
        help="JSON file listing cumulative frequency ranges for each partition")
    parser.add_argument("progress_dir",
        help="directory where progress of the process can be saved")
    parser.add_argument("database",
        help="name of the database (for example: 'steganography')")
    parser.add_argument("dataset",
        help="name of the dataset (for example: 'googlebooks')")
    args = parser.parse_args()

    # Partition the prefixes
    with open(args.ngrams, "r") as f:
        ngram_descriptions = json.load(f)
    partitioned_ngrams = partition_ngrams(ngram_descriptions)

    # Load index ranges
    with open(args.index_ranges, "r") as f:
        index_ranges = json.load(f)

    # Load cumulative frequency ranges
    with open(args.cumfreq_ranges, "r") as f:
        cumfreq_ranges = json.load(f)

    # Connect to the database
    conn = psycopg2.connect(database=args.database)
    cur = conn.cursor()

    # Upload the ngrams
    job_descriptions = gen_job_descriptions(partitioned_ngrams, cumfreq_ranges)
    for job in job_descriptions:
        upload_ngrams(*job)
