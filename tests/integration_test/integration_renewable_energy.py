# Databricks notebook source
# COMMAND ----------
from pyspark import pipelines as dp
from pyspark.sql.functions import count
# COMMAND ----------
catalog: str = spark.conf.get("catalog")
schema: dict = {
            'bronze': spark.conf.get("bronze_schema")
           , 'silver': spark.conf.get("silver_schema")
           , 'gold':  spark.conf.get("gold_schema")
                }
# COMMAND ----------
target_integration_test_row_validation = {
    'dev':{
        'bronze': {
            'bronze_coordinated_solar': 29999,
            'bronze_coordinated_eolic': 29999,
            'bronze_reductions_preliminary_solar': 29999,
            'bronze_reductions_preliminary_eolic': 29999,
            'bronze_real_eolic': 29999,
            'bronze_real_solar': 29999
        },
        'silver':{
            'silver_hub_coordinated': 86,
            'silver_hub_plant': 59998,
            'silver_hub_reductions': 59998,
            'silver_link_measure': 108,
            'silver_sat_measure': 60368
        },
        'gold':{
            'gold_daily_measure': 60368,
            'gold_weekly_measure': 571,
            'gold_monthly_measure': 207
        }
    }
}
target_table_different_percent = 'gold_different_real_coordinated'
# COMMAND ----------

expectation = {"check_silver_calc_columns": {
        "valid percentage": "value_percentage BETWENN 0 AND 100"
        }
    }
# COMMAND ----------
def test_count_table_total_rows(table_name, total_count, target):
    '''
    Count the number of rows in the specified table and compare with the expected values. 
    Fail the update if the count does not match the specified values.
    '''
    @dp.table(
        name=f"{target}.test_{table_name}_total_rows_verification"
    )
    @dp.expect_all_or_fail({"valid count": f"total_rows = {total_count}"}) 
    #TODO change to pyspark function
    def count_table_total_rows():
        return spark.table(f"{target}.{table_name}").select(count("*").alias('total_rows'))
        # return spark.sql(f"""
        #     SELECT COUNT(*) AS total_rows FROM {target}.{table_name}
        # """)
# COMMAND ----------
def test_check_percentage():
    @dp.table(name=f"{schema['gold']}.test_{target_table_different_percent}_total_rows_verification")
    @dp.expect_all_or_fail(expectation['check_silver_calc_columns'])
    def test_check_percentage():
        return spark.table(f"{schema['gold']}.{target_table_different_percent}").select('value_percentage')

# COMMAND ----------
if catalog == 'dev':
    for schema_layer, schema_name in schema.items():
        for bronze_table, size in target_integration_test_row_validation['dev'][schema_layer].items():
            test_count_table_total_rows(bronze_table, size, schema_name)
