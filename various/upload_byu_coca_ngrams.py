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
        
        self.N = int(self.settings["n"])
        
        self.word_columns = tuple("w{}".format(i) for i in range(1,self.N+1))
        self.pos_columns = tuple("pos{}".format(i) for i in range(1,self.N+1))
        self.word_column_names = self.gen_word_column_names(self.N)
        self.pos_column_names = ",".join(self.pos_columns)
        self.word_column_defs = self.gen_word_column_defs(self.N)
        self.pos_column_defs = ",".join(map(lambda x: x + " text", 
                                        self.pos_columns))
        self.lowercase_word_columns_as_normal_columns = ",".join(map(
            lambda x: "lower({x}) AS {x}".format(x=x), self.word_columns))
        self.lowercase_word_columns = ",".join(map(
            lambda x: "lower({})".format(x), self.word_columns))
        
        self.table = "{n}gram_{dataset}".format(**self.settings)
        
    def gen_word_column_names(self, n):
        return ",".join(self.word_columns[0:n])
        
    def gen_word_column_defs(self, n):
        return ",".join(map(lambda x: x + " text", self.word_columns[0:n]))
    
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
            
          CREATE UNIQUE INDEX
            ON "{schema}"."{table}"
            USING btree (i)
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
    
    def marginalize_ngrams(self):
        """
        Creates N tables of marginalised and lowercase ngrams with n in
        {1, ..., N}.
    
        The marginalisation procedure is naive and inexact: (n-1)-gram statistic
        is constructed from (n)-grams by marginalising those whose FIRST (n-1)
        words match the (n-1)-gram exactly.
        """
        
        self.cur.execute("""
          DROP TABLE IF EXISTS "{schema}"."{table}_{N}";
          
          CREATE TABLE "{schema}"."{table}_{N}" (
            i serial primary key,
            {word_column_defs},
            p integer,
            c1 bigint,
            c2 bigint
          );
          
          INSERT INTO
            "{schema}"."{table}_{N}" ( {word_column_names}, p, c1, c2 )
          SELECT
            {word_column_names}, p, c1, c2
          FROM
            (
              SELECT
                min(i) AS i,
                {lowercase_word_columns_as_normal_columns},
                sum(p) AS p,
                min(c1) AS c1,
                max(c2) AS c2
              FROM
                "{schema}"."{table}"
              GROUP BY
                {lowercase_word_columns}
            ) tmp
          ORDER BY
            i ASC;
          
          CREATE UNIQUE INDEX ON "{schema}"."{table}_{N}"
            USING btree (i)
            WITH(fillfactor = 100);
            
          CREATE INDEX ON "{schema}"."{table}_{N}"
            USING btree ({conditional_column_names})
            WITH (fillfactor = 100);
                
          CREATE INDEX ON "{schema}"."{table}_{N}"
            USING btree ({word_column_names})
            WITH (fillfactor = 100);
            
          CREATE UNIQUE INDEX ON "{schema}"."{table}_{N}"
            USING btree (c1)
            WITH(fillfactor = 100);
            
          CREATE UNIQUE INDEX ON "{schema}"."{table}_{N}"
            USING btree (c2)
            WITH(fillfactor = 100);
        """.format(
                schema=self.settings["schema"],
                table=self.table,
                N=self.N,
                word_column_names=self.word_column_names,
                word_column_defs=self.word_column_defs,
                lowercase_word_columns_as_normal_columns=\
                    self.lowercase_word_columns_as_normal_columns,
                lowercase_word_columns=self.lowercase_word_columns,
                conditional_column_names=self.gen_word_column_names(self.N-1)
            )
        )
        
        self.conn.commit()
        
        print("Succesfully saved data to table "
              "\"{schema}\".\"{table}_{N}\".".format(
            schema=self.settings["schema"],
            table=self.table,
            N=self.N,
        ))
        
        for n in range(self.N-1,0,-1):
            self.cur.execute("""
              DROP TABLE IF EXISTS "{schema}"."{table}_{n}";
        
              CREATE TABLE "{schema}"."{table}_{n}" (
                i serial primary key,
                {word_column_defs},
                p integer,
                c1 bigint,
                c2 bigint
              );
        
              INSERT INTO
                "{schema}"."{table}_{n}" ( {word_column_names}, p, c1, c2 )
              SELECT
                {word_column_names}, p, c1, c2
              FROM
                (
                  SELECT
                    min(i) AS i,
                    {word_column_names},
                    sum(p) AS p,
                    min(c1) AS c1,
                    max(c2) AS c2
                  FROM
                    "{schema}"."{table}_{N}"
                  GROUP BY
                    {word_column_names}
                ) tmp
              ORDER BY
                i ASC;
          
              CREATE UNIQUE INDEX ON "{schema}"."{table}_{n}"
                USING btree (i)
                WITH(fillfactor = 100);
                
              CREATE INDEX ON "{schema}"."{table}_{n}"
                USING btree ({word_column_names})
                WITH (fillfactor = 100);
            
              CREATE UNIQUE INDEX ON "{schema}"."{table}_{n}"
                USING btree (c1)
                WITH(fillfactor = 100);
            
              CREATE UNIQUE INDEX ON "{schema}"."{table}_{n}"
                USING btree (c2)
                WITH(fillfactor = 100);
            """.format(
                    schema=self.settings["schema"],
                    table=self.table,
                    n=n,
                    N=n+1,
                    word_column_names=self.gen_word_column_names(n),
                    word_column_defs=self.gen_word_column_defs(n)
                )
            )
            
            if n > 1:
                self.cur.execute("""
                  CREATE INDEX ON "{schema}"."{table}_{n}"
                    USING btree ({conditional_column_names})
                    WITH (fillfactor = 100);
                """.format(
                        schema=self.settings["schema"],
                        table=self.table,
                        n=n,
                        conditional_column_names=self.gen_word_column_names(n-1)
                    )
                )
            
            self.conn.commit()
        
            print("Succesfully saved data to table "
                  "\"{schema}\".\"{table}_{n}\".".format(
                schema=self.settings["schema"],
                table=self.table,
                n=n,
            ))
            
        self.cur.execute("""
          DROP TABLE IF EXISTS "{schema}"."{table}_0";
    
          CREATE TABLE "{schema}"."{table}_0" (
            i serial primary key,
            p integer,
            c1 bigint,
            c2 bigint
          );
          
          INSERT INTO
            "{schema}"."{table}_0" ( p, c1, c2 )
          SELECT
            sum(p) AS p,
            min(c1) AS c1,
            max(c2) AS c2
          FROM
            "{schema}"."{table}_1";
        """.format(
                schema=self.settings["schema"],
                table=self.table
            )
        )

        self.conn.commit()
    
        print("Succesfully saved data to table "
              "\"{schema}\".\"{table}_0\".".format(
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
d.marginalize_ngrams()
d.disconnect()

print("Finished uploading data, remember to run VACUUM to analyse the tables.")
