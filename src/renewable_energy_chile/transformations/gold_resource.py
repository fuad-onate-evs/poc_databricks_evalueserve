# Databricks notebook source
# COMMAND ----------
import dlt
from expectation import rules
from pyspark.sql.functions import *
from pyspark.sql.types import DoubleType
from modules import replace_comma
# COMMAND ----------
# DBTITLE 1,Variables
silver_schema : str = spark.conf.get("silver_schema")
gold_schema : str = spark.conf.get("gold_schema")
# COMMAND ----------
# DBTITLE 1,GOLD_MONTHLY_MEASURE
@dlt.table(
    name=f"{gold_schema}.gold_monthly_measure",
    comment="Calculations of avg value in the month loaded."
)

def gold_monthly_measure():
    df_source = dlt.read(f"{silver_schema}.silver_sat_measure")

    # df_cleaned = df_source \
        # .withColumn("plant_double", replace_comma("plant_value")) \
        # .withColumn("reduction_double", replace_comma("reduction_value")) \
        # .withColumn("coordinated_double", replace_comma("coordinated_value"))

    df_aggregated = df_source.groupBy(
        "hash_measure_key",
        month("record_date").alias("month_val"),
        year("record_date").alias("year_val")
    ).agg(
        coalesce(avg("plant_value"), lit(0.0)).alias("avg_real"),
        coalesce(avg("reduction_value"), lit(0.0)).alias("avg_reductions"),
        coalesce(avg("coordinated_value"), lit(0.0)).alias("avg_coordinated")
    )

    final_df = df_aggregated \
        .withColumns({
            "date_": concat_ws("/", col("month_val"), col("year_val")),
            "diff_real_coordinated": coalesce(col("avg_real") - col("avg_coordinated"), lit(0.0))
        }) 
    
    return final_df.select(
        "hash_measure_key",
        "date_",
        "avg_real",
        "avg_reductions",
        "avg_coordinated",
        "diff_real_coordinated"
    )

# COMMAND ----------
# DBTITLE 1,GOLD_DAILY_MEASURE
@dlt.table(
    name=f"{gold_schema}.gold_daily_measure",
    comment="Calculations of daily average values."
)
def gold_daily_measure():
    df_source = dlt.read(f"{silver_schema}.silver_sat_measure")
    df_aggregated = df_source.groupBy(
        "hash_measure_key",
        "record_date"
    ).agg(
        coalesce(avg("plant_value"), lit(0.0)).alias("avg_real"),
        coalesce(avg("reduction_value"), lit(0.0)).alias("avg_reductions"),
        coalesce(avg("coordinated_value"), lit(0.0)).alias("avg_coordinated")
    )

    final_df = df_aggregated \
        .withColumn("diff_real_coordinated", coalesce(col("avg_real") - col("avg_coordinated"), lit(0.0)))

    return final_df.select(
        "hash_measure_key",
        "record_date",
        "avg_real",
        "avg_reductions",
        "avg_coordinated",
        "diff_real_coordinated"
    )
# COMMAND ----------
# DBTITLE 1,GOLD_WEEKLY_MEASURE
@dlt.table(
    name=f"{gold_schema}.gold_weekly_measure",
    comment="Calculations of weekly average values using ISO 8601 week-numbering calendar."
)
def gold_weekly_measure():
    df_source = dlt.read(f"{silver_schema}.silver_sat_measure")

    df_cleaned = df_source \
        .withColumns({
            "iso_week": weekofyear(col("record_date")).cast(StringType()),
            "iso_year": year(col("record_date")).cast(StringType()),
            "plant_double": col("plant_value"),
            "reduction_double": col("reduction_value"),
            "coordinated_double": col("coordinated_value")
        })

    df_aggregated = df_cleaned.groupBy(
        "hash_measure_key",
        "iso_week",
        "iso_year"
    ).agg(
        bround(coalesce(avg("plant_double"), lit(0.0)),2).alias("avg_real"),
        bround(coalesce(avg("reduction_double"), lit(0.0)),2).alias("avg_reductions"),
        bround(coalesce(avg("coordinated_double"), lit(0.0)),2).alias("avg_coordinated")
    )

    final_df = df_aggregated \
        .withColumns({
            "date_": concat_ws("/", col("iso_week"), col("iso_year")),
            "diff_real_coordinated": coalesce(col("avg_real") - col("avg_coordinated"), lit(0.0))
        })

    return final_df.select(
        "hash_measure_key",
        "date_",
        "avg_real",
        "avg_reductions",
        "avg_coordinated",
        "diff_real_coordinated"
    )
# COMMAND ----------
# DBTITLE 1,DIFFERENT_REAL_COORDINATED
@dlt.table(
    name=f"{gold_schema}.different_real_coordinated",
    comment="Applies rounding and percentage metrics from the daily measure table."
)
def different_real_coordinated():
    df_source = dlt.read(f"{gold_schema}.gold_daily_measure")

    denominator = coalesce(col("avg_real"), lit(0.0))
    numerator = coalesce(col("diff_real_coordinated"), lit(0.0)) * 100

    final_df = df_source \
        .withColumns({
            "value_": bround(col("diff_real_coordinated"), 2), 
            "value_percentage": bround(when(denominator == 0, lit(0.0)).otherwise(numerator / denominator),2)
        })

    return final_df.select(
        col("hash_measure_key").alias("hash_link_key"),
        "record_date",
        "value_",
        "value_percentage"
    )