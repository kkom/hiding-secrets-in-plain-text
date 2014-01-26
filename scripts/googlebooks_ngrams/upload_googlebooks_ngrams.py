#!/usr/bin/env python3

descr = """
This scripts upload Google Books Ngrams to a local PostgreSQL database.
"""

import argparse
import json
import os

import psycopg2

from pysteg.common.files import open_file_to_process, FileAlreadyProcessed
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
    
    def get_table_name(schema, table):
        return "\"{schema}\".\"{table}\"".format(**locals())
    
    def get_column_definitions(n):
        return ",\n".join(map(
            lambda x: "w{} INTEGER".format(x),
            range(1, n+1)
        ))
        
    def get_column_names(n):
        return ", ".join(map(lambda x: "w{}".format(x), range(1, n+1)))
    
    table = get_table_name(args.dataset, "{n}grams".format(**locals()))
    context_table = get_table_name(args.dataset, "{n}grams__context".format(
        **locals()))
        
    column_definitions = get_column_definitions(n)
    context_column_definitions = get_column_definitions(n-1) + ",\n" if n > 1 else ""
    
    columns = get_column_names(n)
    context_columns = get_column_names(n-1) if n > 1 else ""
    
    cur.execute("""
        DROP TABLE IF EXISTS {table} CASCADE;
    
        CREATE TABLE {table} (
          i SERIAL,
          {column_definitions},
          c1 BIGINT,
          c2 BIGINT
        );
        
        DROP TABLE IF EXISTS {context_table} CASCADE;

        CREATE TABLE {context_table} (
          i SERIAL,
          {context_column_definitions}
          c1 BIGINT,
          c2 BIGINT
        );
        """.format(**locals())
    )
    conn.commit()
    print("Created TABLE {table}".format(**locals()))
    print("Created context TABLE {context_table}".format(**locals()))
    
    for partition in sorted(prefixes.keys()):
        partition_table = get_table_name(args.dataset,
            "{n}grams_{partition}".format(**locals()))
            
        index_range = index_ranges[partition]
        cumfreq_range = cumfreq_ranges[partition]
            
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
    
        for prefix in prefixes[partition]:
            path = os.path.join(args.input, ngram_filename(n, prefix))
            
            tmp_table = get_table_name(args.dataset,
                "tmp_{n}grams_{prefix}".format(**locals()))
            
            cur.execute("""
                DROP TABLE IF EXISTS {tmp_table};
            
                CREATE TABLE {tmp_table} (
                  i SERIAL PRIMARY KEY,
                  {column_definitions},
                  f BIGINT
                );
                
                COPY
                  {tmp_table} ({columns}, f)
                FROM
                  %s;
                    
                INSERT INTO
                  {partition_table} ({columns}, c1, c2)
                SELECT
                  {columns},
                  sum(f) OVER (ORDER BY {columns}) + {cumfreq_range[0]} - f AS c1,
                  sum(f) OVER (ORDER BY {columns}) + {cumfreq_range[0]} AS c2
                FROM
                  {tmp_table};
                  
                DROP TABLE {tmp_table};
                """.format(**locals()),
                (path,)
            )
            conn.commit()
            print("Dumped FILE {path} to TABLE {partition_table}".format(
                **locals()))
            
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