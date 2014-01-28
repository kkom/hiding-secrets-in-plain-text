def get_table_name(schema, table):
    return "\"{schema}\".\"{table}\"".format(**locals())
