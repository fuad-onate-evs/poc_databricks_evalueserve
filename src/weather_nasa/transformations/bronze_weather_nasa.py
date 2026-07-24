# Databricks notebook source
# Bronze — historical hourly weather from NASA POWER (measured-equivalent, DMC fields).
import dlt
from pyspark.sql.functions import expr  # noqa: F401

env = spark.conf.get("catalog")
bronze_schema = spark.conf.get("bronze_schema")


@dlt.table(name=f"{bronze_schema}.bronze_weather_nasa_power", table_properties={"quality": "bronze"},
           comment="Raw NASA POWER hourly weather per plant coordinate (Auto Loader).")
def bronze_weather_nasa_power():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("header", "true")
        .load(f"/Volumes/{env}/{bronze_schema}/lookup/nasa_power/")
        .selectExpr(
            "*",
            "_metadata.file_modification_time AS modification_date",
            "'nasa_power' AS source",
        )
    )
