from pyspark.sql.functions import *

def special_characters(col_name: str):
    return(upper(replace(replace(replace(col(col_name), lit(' '), lit('_')), lit('Ñ'), lit('N')), lit('\ufffd'), lit('N'))))

def change_comma_to_dot(col_name: str):
    return regexp_replace(col(col_name), ",", ".").cast("double")