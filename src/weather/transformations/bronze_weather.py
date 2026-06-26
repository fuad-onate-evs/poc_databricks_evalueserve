# Databricks notebook source
# Weather streaming solution — BRONZE.
# Native Databricks Auto Loader (cloudFiles) streaming ingestion of the real DGF
# weather data (solar irradiance + wind) that scripts/dgf_explorador_fetcher.py lands
# as JSON in the UC Volume. API (DGF) -> Volume -> here.
# COMMAND ----------
import dlt
from pyspark.sql.functions import col

catalog: str = spark.conf.get("catalog")
bronze_schema: str = spark.conf.get("bronze_schema")
# COMMAND ----------


@dlt.table(
    name=f"{bronze_schema}.weather_bronze",
    comment="Raw DGF weather (solar irradiance + wind) per plant, landed as JSON by the "
            "fetcher and ingested incrementally with Auto Loader.",
    table_properties={"layer": "bronze", "quality": "bronze", "source": "dgf-explorador"},
)
def weather_bronze():
    landing = f"/Volumes/{catalog}/{bronze_schema}/lookup/dgf/"
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(landing)
        .withColumn("file_path", col("_metadata.file_path"))
        .withColumn("ingested_at", col("_metadata.file_modification_time"))
    )
