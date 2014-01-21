#!/usr/bin/env python3

descr = """
This script will upload all single words from Google Books Ngrams database and
index them alphabetically.
"""

epilog = """
Remember that all file access will be done by the local user postgres. All paths
need to be readable by the user and specified from its perspective.

Optionally, you can run the process from a chosen stage:
  1: Create a temporary table and dump all words to it
  2: Create an index table and insert into it all sorted words from the
     temporary table
  3: If specified, output the index to a text file
"""

import argparse
import os

import psycopg2

# Define and parse arguments
parser = argparse.ArgumentParser(
    description=descr,
    epilog=epilog,
    formatter_class=argparse.RawDescriptionHelpFormatter
)

parser.add_argument("files", nargs='+',
    help="uncompressed text files with the ngrams")
parser.add_argument("database", help="name of the database")
parser.add_argument("dataset",
    help="name of the dataset (for example: 'googlebooks')")
    
parser.add_argument("--stage", type=int,
    help="stage from which to run the script")
parser.add_argument("--output", help="output text file for the index")

args = parser.parse_args()

# Create table names with schemas
tmp_table = "\"{dataset}\".raw_word_indices".format(dataset=args.dataset)
table = "\"{dataset}\".word_indices".format(dataset=args.dataset)

# Connect to the database
conn = psycopg2.connect(database=args.database)
cur = conn.cursor()

# Stage 1: Create a temporary table and dump all words to it
if args.stage <= 1:
    cur.execute("""
        DROP TABLE IF EXISTS {tmp_table};

        CREATE TABLE {tmp_table} (
          i BIGSERIAL PRIMARY KEY,
          w TEXT UNIQUE,
          f BIGINT
        );
        """.format(**locals())
    )
    conn.commit()
    
    print("Created TABLE {tmp_table}".format(**locals()))
    
    for file in args.files:
        # Escape all backslashes
        with open(file, 'rb') as i:
            with open(file + '_ESCAPED', 'wb') as o:
                s = i.read(1)
                backslash = b'\\'
                eof = b''
                while(s != eof):
                    if s != backslash:
                        o.write(s)
                    else:
                        o.write(backslash)
                        o.write(s)
                    s = i.read(1)

        # Dump the escaped file
        cur.execute("""
            COPY {tmp_table} (w, f)
            FROM %s;
            """.format(**locals()),
            (file + "_ESCAPED",)
        )
        conn.commit()
    
        # Remove the escaped file
        os.remove(file + "_ESCAPED")
    
        print("Dumped FILE {file}".format(**locals()))

# Stage 2: Create an index table and insert into it sorted words from the
#          temporary table
if args.stage <= 2:
    cur.execute("""
        DROP TABLE IF EXISTS {table};

        CREATE TABLE {table} (
          i BIGSERIAL PRIMARY KEY,
          w TEXT UNIQUE
        );
    
        INSERT INTO
          {table} (w)
        SELECT
          w
        FROM
          {tmp_table}
        ORDER BY
          w ASC;
      
        CREATE INDEX ON {table}
          USING btree (w)
          WITH (fillfactor = 100);
        """.format(dataset=args.dataset)
    )
    conn.commit()
    
    print("Created TABLE {table}".format(**locals()))
    print("Created INDEX on column \"w\"".format(**locals()))
    
# Stage 3: If specified, output the index to a text file
if args.stage <= 3 and args.output:
    cur.execute("""
        COPY
          {table}
        TO
          %s;
        """.format(**locals()),
        (args.output,)
    )
    conn.commit()
    
# Disconnect from the database
cur.close()
conn.close()
