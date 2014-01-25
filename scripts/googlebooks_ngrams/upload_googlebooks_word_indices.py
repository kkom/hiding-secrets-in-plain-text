#!/usr/bin/env python3

descr = """
This script will upload all single words from Google Books Ngrams database and
index them alphabetically.
"""

epilog = """
Remember that all file access will be done by the local user postgres. All paths
need to be readable by the user and specified from its perspective.

Optionally, you can run the process from a chosen stage:
  1: Create the index table from 1grams
  2: If specified, output the index to a text file
"""

import argparse
import json
import os
import tempfile

import psycopg2

from pysteg.common.files import path_append_flag
from pysteg.googlebooks_ngrams.ngrams_analysis import ngram_filename

# Define and parse arguments
parser = argparse.ArgumentParser(
    description=descr,
    epilog=epilog,
    formatter_class=argparse.RawDescriptionHelpFormatter
)
parser.add_argument("ngrams", help="JSON file listing all the ngram files")
parser.add_argument("input", help="input directory of ngram files")
parser.add_argument("database",
    help="name of the database (for example: 'steganography')")
parser.add_argument("dataset",
    help="name of the dataset (for example: 'googlebooks')")
parser.add_argument("--stage", type=int, default=1,
    help="stage from which to run the script")
parser.add_argument("--index_output", help="output text file for the index")
parser.add_argument("--index_ranges_output",
    help="output text file with index ranges for different prefixes")
args = parser.parse_args()

# Create shortcuts for special characters (as bytes)
backslash = b'\\'
eof = b''

# Create table names with schemas
tmp_table = "\"{dataset}\".tmp_word_indices".format(dataset=args.dataset)
table = "\"{dataset}\".word_indices".format(dataset=args.dataset)

# Connect to the database
conn = psycopg2.connect(database=args.database)
cur = conn.cursor()

# Stage 1: Create the index table from 1grams
if args.stage <= 1:
    last_indices = {}
    
    # Create a temporary table
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
    
    with open(args.ngrams, 'r') as f:
        ngrams = json.load(f)
    
    # Load words from 1grams into the table
    for prefix in sorted(ngrams["1"]):
        # _START_ and _END_ markers only exist in the context of another ngram.
        # They will not be found in 1grams, so we need to add them manually
        if prefix == "punctuation":
            cur.execute("""
                INSERT INTO
                  {tmp_table} (w, f)
                VALUES
                  ('_START_', NULL),
                  ('_END_', NULL);
                """.format(**locals())
            )
            conn.commit()
            
        # Escape all backslashes
        path = os.path.join(args.input, ngram_filename(1, prefix))
        escaped_path = path_append_flag(path, "_ESCAPED")
        
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

        # Copy words from the escaped file
        cur.execute("""
            COPY
                {tmp_table} (w, f)
            FROM
                %s;
            """.format(**locals()),
            (escaped_path,)
        )
        conn.commit()
    
        # Remove the escaped file
        os.remove(escaped_path)
        
        # Get end index for this prefix
        cur.execute("""
            SELECT currval('{tmp_table}_i_seq');
        """.format(**locals()))
        last_indices[prefix] = cur.fetchone()[0];
        
        print("Inserted words from FILE {path}".format(**locals()))
    
    # Copy data to the actual index table (without frequencies)
    cur.execute("""
        DROP TABLE IF EXISTS {table};

        CREATE TABLE {table} (
          i BIGINT PRIMARY KEY,
          w TEXT UNIQUE
        );
    
        INSERT INTO
          {table} (i, w)
        SELECT
          i, w
        FROM
          {tmp_table}
        ORDER BY
          i ASC;
          
        DROP TABLE {tmp_table};
      
        CREATE INDEX ON {table}
          USING btree (w)
          WITH (fillfactor = 100);
        """.format(**locals())
    )
    conn.commit()
    
    print("Created TABLE {table}".format(**locals()))
    print("Copied data from {tmp_table} to {table}".format(**locals()))
    print("Dropped TABLE {tmp_table}".format(**locals()))
    print("Created INDEX on column \"w\" in TABLE {table}".format(**locals()))
    
    # Calculate the range of indices for each prefix
    if args.index_ranges_output:
        index_ranges = {}
        last_index = 0
        for prefix in sorted(last_indices.keys()):
            index_ranges[prefix] = (last_index+1, last_indices[prefix])
            last_index = last_indices[prefix]
            
        with open(args.index_ranges_output, "w") as f:
            json.dump(index_ranges, f)
        
        print("Dumped index ranges to FILE {args.index_ranges_output}".format(
            **locals()))
    
# Stage 2: If specified, output the index to a text file
if args.stage <= 3 and args.index_output:
    tmp_output_path = path_append_flag(args.index_output, "_TMP")
    cur.execute("""
        COPY
          {table}
        TO
          %s;
        """.format(**locals()),
        (tmp_output_path,)
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
    with open(tmp_output_path, "rb") as i:
        with open(args.index_output, "wb") as o:
            s = i.read(1)
            while(s != eof):
                if s != backslash:
                    o.write(s)
                else:
                    o.write(s)
                    i.read(1)
                s = i.read(1)
    os.remove(tmp_output_path)
    print("Dumped words index to FILE {args.index_output}".format(**locals()))
    
# Disconnect from the database
cur.close()
conn.close()
