from pyspark.testing.utils import assertDataFrameEqual
from pyspark.sql.functions import monotonically_increasing_id
from helper import algebraic_operations


def test_distance_row(spark):
    "test window distance"

    data = [
        (1, 1, 1),
        (1, 2, 2), 
        (1, 3, 3), 
        (2, 1, 1), 
        (2, 1, 2), 
        (2, 2, 3),
        (3, None, 1), 
        (3, 1, 2), 
        (3, 2, 3),
        (4, 1, 1), 
        (4, 2, 2), 
        (4, 3, None),
        (5, 1, 1), 
        (5, 2, 2), 
        (5, None, None)
    ] 
    sdf_sample_comma = spark.createDataFrame(data, ["col1", "col2", "col3"])
    sdf_actual = sdf_sample_comma.select(monotonically_increasing_id().alias('id'),
        algebraic_operations.Algebraic_Operation(partition_key='col1', order_key='col2')
        .distance_row('col3').alias('column_distance'))

    expected_df = spark.createDataFrame(
    [
        (0,None),
        (1,1),
        (2,1),
        (3,None),
        (4,1),
        (5,1),
        (6,None),
        (7,1),
        (8,1),
        (9,None),
        (10,1),
        (11,None),
        (12,None),
        (13,None),
        (14,1)      
    ],
    schema="id LONG, column_distance LONG"
    )
    assertDataFrameEqual(sdf_actual, expected_df)