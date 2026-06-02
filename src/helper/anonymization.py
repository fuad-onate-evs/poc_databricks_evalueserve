from pyspark.sql.functions import Column, sha1, concat_ws

def hash_key(*col_names: str) -> Column : 
    return sha1(concat_ws('|', *col_names))