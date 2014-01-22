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

from pysteg.common.files import path_append_hidden_flag

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
    
parser.add_argument("--stage", type=int, default=1,
    help="stage from which to run the script")
parser.add_argument("--output", help="output text file for the index")

args = parser.parse_args()

# Create shortcuts for special characters (as bytes)
backslash = b'\\'
eof = b''

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
    
    for path in args.files:
        # Google Books ngrams represent strings exactly. There are no special
        # characters that need to be escaped, hence \ is a valid character on
        # its own.
        #
        # Conversely, PostgreSQL expects all special characters in data to be
        # escaped, including \. As a result, each backslash needs to be doubled.
        
        # Escape the backslashes to a separate, temporary file
        escaped_path = path_append_hidden_flag(path, "_ESCAPED")
        with open(path, "rb") as i:
            with open(escaped_path, "wb") as o:
                s = i.read(1)
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
            (escaped_path,)
        )
        conn.commit()
    
        # Remove the escaped file
        os.remove(escaped_path)
    
        print("Dumped FILE {path} to TABLE {tmp_table}".format(**locals()))

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
    print("Inserted sorted data from {tmp_table} to {table}".format(**locals()))
    print("Created INDEX on column \"w\" in TABLE {table}".format(**locals()))
    
# Stage 3: If specified, output the index to a text file
if args.stage <= 3 and args.output:
    cur.execute("""
        COPY
          {table}
        TO
          %s;
        """.format(**locals()),
        (args.output + "_TMP",)
    )
    conn.commit()
    
    # PostgreSQL's COPY TO statement will return all special characters,
    # including backslash, escaped with an extra backslash. Since Google Books
    # ngrams do not contain any special characters and backlash is considered to
    # be a normal character, this extra backslash is unnecessary and will always
    # occur before another backslash.
    #
    # So whenever a backslash is read from the file output by PostgreSQL, it
    # will be followed by an unnecessary one.
    with open(args.output + "_TMP", 'rb') as i:
        with open(args.output, 'wb') as o:
            s = i.read(1)
            while(s != eof):
                if s != backslash:
                    o.write(s)
                else:
                    o.write(s)
                    i.read(1)
                s = i.read(1)
    os.remove(args.output + "_TMP")
    
# Disconnect from the database
cur.close()
conn.close()
