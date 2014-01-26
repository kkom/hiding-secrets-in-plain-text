#!/usr/bin/env python3

descr = """
Lol
"""

epilog = """
Wtf
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
    
    table = "\"{dataset}\".\"{n}grams\"".format(
        dataset=args.dataset, **locals())
        
    column_definitions = ",\n".join(map(
        lambda x: "w{} INTEGER".format(x),
        range(1, n+1)
    ))
    columns = ", ".join(map(lambda x: "w{}".format(x), range(1, n+1)))
    
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
    
    for partition in sorted(prefixes.keys()):
        partition_table = "\"{dataset}\".\"{n}grams_{partition}\"".format(
            dataset=args.dataset, **locals())
            
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
    
        for prefix in prefixes[partition]:
            path = os.path.join(args.input, ngram_filename(n, prefix))
            
            tmp_table = "\"{dataset}\".\"tmp_{n}grams_{prefix}\"".format(
                dataset=args.dataset, **locals())
            
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
            
            print("Dumped FILE {path}".format(**locals()))
            
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
        print("Created INDEX on ({columns}) in TABLE {partition_table}".format(
            **locals()))

if __name__ == '__main__':
    # Define and parse arguments
    parser = argparse.ArgumentParser(
        description=descr,
        epilog=epilog,
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