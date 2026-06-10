from pyspark.sql import Window
from pyspark.sql.functions import Column, lag, col

class Algebraic_Operation:
    "Make column algebraic operations"

    def __init__(self, partition_key, order_key):
        self.partition_key = partition_key
        self.order_key = order_key

    def distance_row(self, col_name: str) -> Column:
        """
        calculate the numeric  difference between two consecutive rows
        Arg:
            col_name: name of the table column
        Return:
            column with the difference
        """
        
        window_function = Window.partitionBy(self.partition_key).orderBy(self.order_key)
        return col(col_name) - lag(col(col_name)).over(window_function)