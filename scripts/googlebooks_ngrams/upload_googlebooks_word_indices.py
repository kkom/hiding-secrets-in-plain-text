#!/usr/bin/env python3

descr = """
This script will upload all single words from Google Books Ngrams database and
index them alphabetically.
"""

epilog = """
Remember that all file access will be done by the local user postgres. All paths
need to be readable by the user and specified from its perspective.

Optionally, you can run the process from a chosen stage:
  1: Collect all possible words from the ngrams, create a temporary table and
     dump the words to it
  2: Create an index table and insert into it all sorted words from the
     temporary table
  3: If specified, output the index to a text file
"""

import argparse
import os
import tempfile

import psycopg2

from pysteg.googlebooks_ngrams.ngrams_analysis import gen_ngram_descriptions
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

# Stage 1: Collect all possible words from the ngrams, create a temporary table
#          and dump the words to it
if args.stage <= 1:
    # Create a set of all words in the Google Ngrams Database
    words = set()
    for ngram in gen_ngram_descriptions(args.ngrams):
        path = os.path.join(args.input, ngram_filename(*ngram))
        with open(path, "r") as f:
            for line in f:
                words.update(line.split("\t")[:-1])
        print("Read words from FILE {path}".format(**locals()))
    
    # Dump the words to a temporary file escaping all backslashes
    tmp_file = tempfile.NamedTemporaryFile(mode="w", delete=False)            
    for word in words:
        tmp_file.write(word.replace("\\", "\\\\") + "\n")
    tmp_file.close()
    
    # Set 
    os.chmod(tmp_file.name, stat.S_IRGRP)
    os.chmod(tmp_file.name, stat.S_IROTH)
    
    print("Created FILE {tmp_file.name}".format(**locals()))
    
    # Upload the words to a temporary table
    cur.execute("""
        DROP TABLE IF EXISTS {tmp_table};

        CREATE TABLE {tmp_table} (
          i BIGSERIAL PRIMARY KEY,
          w TEXT UNIQUE
        );
        
        COPY
          {tmp_table} (w)
        FROM
          %s;
        """.format(**locals()),
        (tmp_file.name,)
    )
    conn.commit()
    
    print("Created TABLE {tmp_table}".format(**locals()))
    print("Dumped FILE {tmp_file.name} to TABLE {tmp_table}".format(**locals()))
    
    os.remove(tmp_file.name)
    print("Deleted FILE {tmp_file.name}".format(**locals()))

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
        """.format(**locals())
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
    with open(args.output + "_TMP", "rb") as i:
        with open(args.output, "wb") as o:
            s = i.read(1)
            while(s != eof):
                if s != backslash:
                    o.write(s)
                else:
                    o.write(s)
                    i.read(1)
                s = i.read(1)
    os.remove(args.output + "_TMP")
    print("Dumped the words index to FILE {args.output}".format(**locals()))
    
# Disconnect from the database
cur.close()
conn.close()
