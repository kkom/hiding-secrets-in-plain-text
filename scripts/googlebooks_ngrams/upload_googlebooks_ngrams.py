#!/usr/bin/env python3

descr = """
This scripts upload Google Books Ngrams to a local PostgreSQL database.
"""

import argparse
import json
import os

import psycopg2

from pysteg.googlebooks import get_partition
from pysteg.googlebooks_ngrams.ngrams_analysis import ngram_filename

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
    
def upload_ngrams(n, prefixes, index_ranges, cumfreq_ranges):
    """Upload ngrams for a particular n to the PostgreSQL database."""
    
    # Define functions to generate table and columns definitions
    def get_table_name(schema, table):
        return "\"{schema}\".\"{table}\"".format(**locals())
    
    def get_column_definitions(n):
        return ",\n".join(map(
            lambda x: "w{} INTEGER".format(x),
            range(1, n+1)
        ))
        
    def get_column_names(n):
        return ", ".join(map(lambda x: "w{}".format(x), range(1, n+1)))
    
    # Generate table and columns definitions
    table = get_table_name(args.dataset, "{n}grams".format(**locals()))
    context_table = get_table_name(args.dataset, "{n}grams__context".format(
        **locals()))  
    column_definitions = get_column_definitions(n)
    columns = get_column_names(n)
    
    # Create parent ngrams table
    cur.execute("""
        DROP TABLE IF EXISTS {table} CASCADE;
    
        CREATE TABLE {table} (
          i SERIAL,
          {column_definitions},
          c1 BIGINT,
          c2 BIGINT
        );
        """.format(**locals())
    )
    conn.commit()
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
              c1 BIGINT,
              c2 BIGINT
            );
            """.format(**locals())
        )
        conn.commit()
    else:
        cur.execute("""
            DROP TABLE IF EXISTS {context_table};

            CREATE TABLE {context_table} (
              i SERIAL PRIMARY KEY,
              c1 BIGINT,
              c2 BIGINT
            );
            """.format(**locals())
        )
        conn.commit()
    print("Created context TABLE {context_table}".format(**locals()))
    
    # Populate respective partition tables 
    for partition in sorted(prefixes.keys()):
        # Define various properties of the partition table, such as its name and
        # the range of data it is supposed to contain
        partition_table = get_table_name(args.dataset,
            "{n}grams_{partition}".format(**locals()))
        index_range = index_ranges[partition]
        cumfreq_range = cumfreq_ranges[partition]
        
        # Create the partition table
        cur.execute("""
            DROP TABLE IF EXISTS {partition_table};
        
            CREATE TABLE {partition_table} (
              PRIMARY KEY (i),
              CHECK (     w1 >= {index_range[0]}
                      AND w1 <= {index_range[1]}
                      AND c1 >= {cumfreq_range[0]}
                      AND c1 <= {cumfreq_range[1]}
                      AND c2 >= {cumfreq_range[0]}
                      AND c2 <= {cumfreq_range[1]} )
            ) INHERITS ({table});
            """.format(**locals())
        )
        conn.commit()
        print("Created partition TABLE {partition_table}".format(**locals()))
        
        # If n > 1, then data in the context table should be partitioned as well
        if n > 1:        
            context_partition_table = get_table_name(args.dataset,
                "{n}grams_{partition}__context".format(**locals()))
    
            cur.execute("""
                DROP TABLE IF EXISTS {context_partition_table};
        
                CREATE TABLE {context_partition_table} (
                  PRIMARY KEY (i),
                  CHECK (     w1 >= {index_range[0]}
                          AND w1 <= {index_range[1]}
                          AND c1 >= {cumfreq_range[0]}
                          AND c1 <= {cumfreq_range[1]}
                          AND c2 >= {cumfreq_range[0]}
                          AND c2 <= {cumfreq_range[1]} )
                ) INHERITS ({context_table});
                """.format(**locals())
            )
            conn.commit()
            print("Created context partition TABLE "
                  "{context_partition_table}".format(**locals()))
    
        for prefix in prefixes[partition]:
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
                  i INTEGER PRIMARY KEY,
                  {column_definitions},
                  c1 BIGINT,
                  c2 BIGINT
                );
                
                COPY
                  {raw_tmp_table} ({columns}, f)
                FROM
                  %s;
                    
                INSERT INTO
                  {cumfreq_tmp_table} (i, {columns}, c1, c2)
                SELECT
                  i,
                  {columns},
                  sum(f) OVER (ORDER BY {columns}) - f AS c1,
                  sum(f) OVER (ORDER BY {columns}) AS c2
                FROM
                  {raw_tmp_table};
                  
                DROP TABLE {raw_tmp_table};
                """.format(**locals()),
                (path,)
            )
            conn.commit()
            print("Dumped FILE {path} to TABLE {cumfreq_tmp_table}".format(
                **locals()))
        
        # Index the ngrams partition table for efficient access
        cur.execute("""
            CREATE INDEX ON {partition_table}
                USING btree ({columns})
                WITH (fillfactor = 100);
                
            CREATE INDEX ON {partition_table}
                USING btree (c1)
                WITH (fillfactor = 100);
                
            CREATE INDEX ON {partition_table}
                USING btree (c2)
                WITH (fillfactor = 100);
            """.format(**locals())
        )
        conn.commit()
        print("Created INDEXES on ({columns}), (c1) and (c2) in TABLE "
              "{partition_table}".format(**locals()))

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
