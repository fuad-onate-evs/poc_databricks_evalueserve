from pyspark.sql.functions import Column, lower, regexp_replace, col, upper, replace, lit

def generalize_str(col_name: str) -> Column:
    """
    Generalize the string values to a basic form and delete all of the
    special character
    Arg:
     col_name: name of the column
    Return:
        Spark column
    """
    return lower(regexp_replace(col_name, '[^a-zA-Z0-9]', ''))

def special_characters(col_name: str):
    return(upper(replace(replace(replace(col(col_name), lit(' '), lit('_')), lit('Ñ'), lit('N')), lit('\ufffd'), lit('N'))))

def change_comma_to_dot(col_name: str):
    return regexp_replace(col(col_name), ",", ".").cast("double")