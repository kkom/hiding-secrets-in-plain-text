descr = """
This script uploads ngrams from the Brigham Young University (BYU) Corpus of 
Contemporary American English (COCA) database to a PostgreSQL database.

It uses Unix sockets and peer authentication to connect to the 
database, so has to be run on the same machine that the database engine. The
COPY  command will not work unless the user connected to the database is a 
superuser.
"""

import argparse

import psycopg2

class ByuCocaNgramUpload:
    def __init__(self, settings,):
        self.settings = settings
        
        n = int(self.settings["n"])
        
        self.word_columns = tuple("w{}".format(i) for i in range(1,n+1))
        self.pos_columns = tuple("pos{}".format(i) for i in range(1,n+1))
        self.word_column_names = ",".join(self.word_columns)
        self.pos_column_names = ",".join(self.pos_columns)
        self.word_column_defs = ",".join(map(lambda x: x + " text",
                                         self.word_columns))
        self.pos_column_defs = ",".join(map(lambda x: x + " text", 
                                        self.pos_columns))
        
        self.table = "{n}gram_{dataset}".format(**self.settings)
    
    def connect(self):
        self.conn = psycopg2.connect(database=self.settings["database"])
        self.cur = self.conn.cursor()
        
    def disconnect(self):
        self.conn.commit()
        self.cur.close()
        self.conn.close()

    def dump_data(self):
        self.cur.execute("""
          DROP TABLE IF EXISTS "{schema}"."raw_{table}";

          CREATE TABLE "{schema}"."raw_{table}" (
            i serial primary key,
            p integer,
            {word_column_defs},
            {pos_column_defs}
          );
          
          COPY "{schema}"."raw_{table}" (
            p,
            {word_column_names},
            {pos_column_names}
          )
          FROM
            %s;
        """.format(
                schema=self.settings["schema"],
                table=self.table,
                word_column_defs=self.word_column_defs,
                pos_column_defs=self.pos_column_defs,
                word_column_names=self.word_column_names,
                pos_column_names=self.pos_column_names
            ), (self.settings["file"],)
        )
        
        self.conn.commit()

        print("Succesfully dumped data to a temporary table "
              "\"{schema}\".\"raw_{table}\".".format(
            schema=self.settings["schema"],
            table=self.table
        ))
        
    def cumulate_data(self):
        self.cur.execute("""
          DROP TABLE IF EXISTS "{schema}"."{table}";
          
          CREATE TABLE "{schema}"."{table}" (
            i integer primary key,
            {word_column_defs},
            {pos_column_defs},
            p integer,
            c1 bigint,
            c2 bigint
          );
          
          INSERT INTO
            "{schema}"."{table}"
          SELECT
            i,
            {word_column_names},
            {pos_column_names},
            p,
            sum(p) OVER (ORDER BY i) - p AS c1,
            sum(p) OVER (ORDER BY i) AS c2
          FROM
            "{schema}"."raw_{table}";
            
          DROP TABLE "{schema}"."raw_{table}";
            
          CREATE UNIQUE INDEX "{schema}_{table}_i"
            ON "{schema}"."{table}"
            USING btree (i)
            WITH(fillfactor = 100);
            
          CREATE UNIQUE INDEX "{schema}_{table}_c1"
            ON "{schema}"."{table}"
            USING btree (c1)
            WITH(fillfactor = 100);
            
          CREATE UNIQUE INDEX "{schema}_{table}_c2"
            ON "{schema}"."{table}"
            USING btree (c2)
            WITH(fillfactor = 100);
        """.format(
                schema=self.settings["schema"],
                table=self.table,
                word_column_defs=self.word_column_defs,
                pos_column_defs=self.pos_column_defs,
                word_column_names=self.word_column_names,
                pos_column_names=self.pos_column_names
            )
        )
        
        self.conn.commit()

        print("Succesfully saved data to table "
              "\"{schema}\".\"{table}\".".format(
            schema=self.settings["schema"],
            table=self.table
        ))

settings = {
    "database": "steganography",
    "schema": "byu_coca_corpus",
}

parser = argparse.ArgumentParser(
    description=descr,
    formatter_class=argparse.RawDescriptionHelpFormatter
)

parser.add_argument("file", help="uncompressed text file with the ngrams")
parser.add_argument("n", type=int, help="size of the ngrams")
parser.add_argument("dataset",
    help="name of the dataset (for example: '1m_sample')")

args = parser.parse_args()

settings["file"] = args.file
settings["n"] = args.n
settings["dataset"] = args.dataset

d = ByuCocaNgramUpload(settings)
d.connect()
d.dump_data()
d.cumulate_data()
d.disconnect()
