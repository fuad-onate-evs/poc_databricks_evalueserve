from pyspark.sql.functions import Column, sha1, concat_ws

def hash_key(*col_names: str) -> Column : 
    "generate a hask key from multiple columns using sha-1"
    
    return sha1(concat_ws('|', *col_names))