import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import DoubleType, StringType
from pyspark.sql.functions import udf



@udf(returnType=DoubleType())
def replace_comma(string_val):
    if string_val is None:
        return None
    try:
        return float(string_val.replace(",", "."))
    except (ValueError, AttributeError):
        return None
    

@udf(returnType=StringType())
def replace_N_char(string_val):
    if string_val is None:
        return None
    try:
        # Use Python string methods instead of PySpark functions
        result = string_val.upper().replace('Ñ', 'N').replace('\ufffd', 'N')
        return result
    except (ValueError, AttributeError):
        return None