# Databricks notebook source
# Gold — marts for the NASA weather domain: resource KPI, daily and monthly series.
import dlt
from pyspark.sql.functions import avg, col, count, max as smax, rank, round as rnd, sum as ssum, to_date, when
from pyspark.sql.window import Window

silver_schema = spark.conf.get("silver_schema")
gold_schema = spark.conf.get("gold_schema")


def _fact_dim():
    f = dlt.read(f"{silver_schema}.silver_weather_nasa_fact_hourly")
    d = dlt.read(f"{silver_schema}.silver_weather_nasa_dim_plant")
    return f.join(d, "plant_key")


@dlt.table(name=f"{gold_schema}.gold_weather_nasa_resource_kpi",
           table_properties={"quality": "gold"},
           comment="Per-plant resource KPI over the full history: metric, rank, A/B/C tier.")
def gold_weather_nasa_resource_kpi():
    agg = (
        _fact_dim().groupBy("plant", "resource").agg(
            rnd(avg(when((col("ghi") > 0), col("ghi"))), 1).alias("avg_ghi_daytime"),
            rnd(avg("viento_50m"), 2).alias("avg_wind_50m"),
            rnd(avg("temperatura"), 1).alias("avg_temp"),
            count("*").alias("hours"),
        )
        .withColumn("metric", when(col("resource") == "solar", col("avg_ghi_daytime")).otherwise(col("avg_wind_50m")))
    )
    w = Window.partitionBy("resource").orderBy(col("metric").desc())
    return (
        agg.withColumn("rank", rank().over(w))
        .withColumn("tier", when(col("rank") <= 2, "A").when(col("rank") <= 4, "B").otherwise("C"))
        .select("plant", "resource", "metric", "rank", "tier",
                "avg_ghi_daytime", "avg_wind_50m", "avg_temp", "hours")
    )


@dlt.table(name=f"{gold_schema}.gold_weather_nasa_daily",
           table_properties={"quality": "gold"}, comment="Per-plant daily weather aggregates.")
def gold_weather_nasa_daily():
    return (
        _fact_dim().withColumn("fecha", to_date("momento"))
        .groupBy("plant", "resource", "fecha").agg(
            rnd(avg("ghi"), 1).alias("avg_ghi"),
            rnd(smax("ghi"), 1).alias("max_ghi"),
            rnd(avg("viento_50m"), 2).alias("avg_wind_50m"),
            rnd(avg("temperatura"), 1).alias("avg_temp"),
            rnd(ssum("agua_caida"), 1).alias("precip_mm"),
        )
    )


@dlt.table(name=f"{gold_schema}.gold_weather_nasa_monthly",
           table_properties={"quality": "gold"}, comment="Per-plant monthly weather aggregates.")
def gold_weather_nasa_monthly():
    return (
        _fact_dim().groupBy("plant", "resource", "anio", "mes").agg(
            rnd(avg("ghi"), 1).alias("avg_ghi"),
            rnd(avg("viento_50m"), 2).alias("avg_wind_50m"),
            rnd(avg("temperatura"), 1).alias("avg_temp"),
            rnd(ssum("agua_caida"), 1).alias("precip_mm"),
        )
    )
