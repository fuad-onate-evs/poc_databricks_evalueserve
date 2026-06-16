from pyspark.testing.utils import assertDataFrameEqual
from helper import string_transformation

def test_change_comma_to_dot(spark):
    "check the funtion change_comma_to_dot"

    data = [
        ('123,34',),
        ('22.33',), 
        ('9090',), 
        ('222.',), 
        (None,)
    ] 
    sdf_sample_comma = spark.createDataFrame(data, ["string_value"])
    sdf_actual = sdf_sample_comma.select(string_transformation
        .change_comma_to_dot('string_value').alias('string_value'))

    expected_df = spark.createDataFrame(
    [
        (123.34),
        (22.33),
        (9090),
        (222),
        (None)
    ],
    schema="string_value DOUBLE"
    )
    assertDataFrameEqual(sdf_actual, expected_df)

def test_generalize_str(spark):
    "check the generalization of string values"

    data = [
        ('df234;.',),
        ('werds^&,',), 
        ('asdffs ds',), 
        ('dfgs  ',),
        ('hola',), 
        (None,)
    ]
    sdf_sample_comma = spark.createDataFrame(data, ["string_generalize"])
    sdf_actual = sdf_sample_comma.select(string_transformation
        .generalize_str('string_generalize').alias('string_generalize'))
    
    expected_df = spark.createDataFrame(
        [
        ('df234',),
        ('werds',), 
        ('asdffsds',), 
        ('dfgs',),
        ('hola',), 
        (None,)
        ],
        schema="string_generalize STRING"
        )
    assertDataFrameEqual(sdf_actual, expected_df)

def test_specail_characters(spark):
    "check characters ' ', 'Ñ', '\ufffd'"

    data = [
        ('on e',),
        ('twoÑ',), 
        ('threÑe',), 
        ('fou\ufffdr',),
        (None,)
    ]
    sdf_sample_comma = spark.createDataFrame(data, ["string_special"])
    sdf_actual = sdf_sample_comma.select(string_transformation
        .generalize_str('string_special').alias('string_special'))
    
    expected_df = spark.createDataFrame(
        [
        ('one',),
        ('two',), 
        ('three',), 
        ('four',),
        (None,)
        ],
        schema="string_special STRING"
        )
    assertDataFrameEqual(sdf_actual, expected_df)
