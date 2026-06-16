from pyspark.testing.utils import assertDataFrameEqual
from helper import anonymization

def test_hash_key(spark):
    ""
    data = [
        ('one','two', 'three'),
        ('one', 'two', 'thre'), 
        ('one|','two', 'three'), 
        ('three', 'two', 'one'), 
        (None, 'one', 'two'), 
        (None, None, None)
    ] 
    sdf_sample_comma = spark.createDataFrame(data, ["col1", "col2", "col3"])
    sdf_actual = sdf_sample_comma.select(anonymization
        .hash_key('col1', 'col2', 'col3').alias('hash_column'))

    expected_df = spark.createDataFrame(
    [
        ('3e41c7661fed23adc5be0c213bc31fc5c524a0b6'),
        ('fc72552505dfb7ebc516ac8cc1950360ccb5c19c'),
        ('0a150a934701279089b8d5ab02097a0bfa19ac9e'),
        ('142e5275c8c1b795063af368326b5d3fc0269c30'),
        ('10aca8d9cdb2f7da6affcdb2a5889e1496047bc4'),
        ('da39a3ee5e6b4b0d3255bfef95601890afd80709')
    ],
    schema="hash_column STRING"
    )
    assertDataFrameEqual(sdf_actual, expected_df)