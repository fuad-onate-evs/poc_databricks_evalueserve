# Databricks notebook source
# Weather streaming solution — GOLD (the final layer the review asks for).
# Per-plant resource KPI: headline metric, ranking within resource, and a quality tier.
# COMMAND ----------
import dlt
from pyspark.sql.functions import col, when, lit, round as sround, rank
from pyspark.sql.window import Window

silver_schema: str = spark.conf.get("silver_schema")
gold_schema: str = spark.conf.get("gold_schema")
# COMMAND ----------


@dlt.table(
    name=f"{gold_schema}.weather_gold_resource_kpi",
    comment="Renewable-resource KPI per plant from real DGF data: headline metric "
            "(solar GHI / wind speed), rank within its resource, and an A/B/C quality tier.",
    table_properties={"layer": "gold", "quality": "gold"},
)
def weather_gold_resource_kpi():
    df = dlt.read(f"{silver_schema}.weather_silver")
    metric = when(col("resource") == "solar", col("ghi")).otherwise(col("wind_ms"))
    ranked = Window.partitionBy("resource").orderBy(metric.desc())
    tier = (
        when(col("resource") == "solar",
             when(col("ghi") >= 6.5, "A - excellent")
             .when(col("ghi") >= 5.5, "B - good")
             .otherwise("C - moderate"))
        .otherwise(
             when(col("wind_ms") >= 7, "A - excellent")
             .when(col("wind_ms") >= 5, "B - good")
             .otherwise("C - moderate"))
    )
    return (
        df.withColumn("resource_metric", sround(metric, 2))
        .withColumn("unit", when(col("resource") == "solar", lit("GHI kWh/m2/day")).otherwise(lit("wind m/s")))
        .withColumn("temp_c", sround(col("temp_c"), 1))
        .withColumn("rank_in_resource", rank().over(ranked))
        .withColumn("resource_tier", tier)
        .select("plant", "resource", "lat", "lon", "resource_metric", "unit",
                "temp_c", "rank_in_resource", "resource_tier")
    )
