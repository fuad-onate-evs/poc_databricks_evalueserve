# Databricks notebook source
# Option A (recommended for hourly) — DGF meteorological bronze via Auto Loader.
#
# The poller (scripts/dgf_poller.py) lands DGF JSON in a UC Volume; this DLT table
# ingests it with the SAME Auto Loader pattern as the existing bronze, so the new
# source needs no new infrastructure. See docs/card-56-bronze-landing-design.md §3.
#
# NOT wired into the pipeline yet (the pipeline glob only includes transformations/**).
# To activate, see src/renewable_energy_chile/streaming/README.md.
# COMMAND ----------
import dlt
from pyspark.sql.functions import *

env: str = spark.conf.get("catalog")
bronze_schema: str = spark.conf.get("bronze_schema")

# Expected hourly DGF/met record shape. Following the project convention, pin this
# as an explicit `schema=` with COMMENTs once the real api.minenergia.cl /api/ payload
# is confirmed (after approval). Until then we infer, exactly like the other bronze
# tables started:
#
#   DGF_SCHEMA = (
#       "station_id STRING COMMENT 'DGF station identifier.', "
#       "station_name STRING COMMENT 'Station name / location.', "
#       "timestamp TIMESTAMP COMMENT 'Observation/forecast time (hourly).', "
#       "latitude DOUBLE COMMENT 'Station latitude.', "
#       "longitude DOUBLE COMMENT 'Station longitude.', "
#       "ghi DOUBLE COMMENT 'Global horizontal irradiance (W/m^2).', "
#       "dni DOUBLE COMMENT 'Direct normal irradiance (W/m^2).', "
#       "wind_speed DOUBLE COMMENT 'Wind speed (m/s).', "
#       "wind_dir DOUBLE COMMENT 'Wind direction (degrees).', "
#       "temperature DOUBLE COMMENT 'Air temperature (C).'"
#   )
# COMMAND ----------


@dlt.table(
    name=f"{bronze_schema}.bronze_dgf_met",
    comment="Raw DGF (U. de Chile Geophysics) hourly meteorological data — solar irradiance / "
            "wind — landed as JSON by scripts/dgf_poller.py and ingested with Auto Loader "
            "(card #56, landing Option A).",
    table_properties={"layer": "bronze", "quality": "bronze", "source": "dgf"},
)
def bronze_dgf_met():
    landing = f"/Volumes/{env}/{bronze_schema}/lookup/dgf/"
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(landing)
        .selectExpr(
            "*",
            "_metadata.file_modification_time AS modification_date",
            "_metadata.file_path AS file_path",
            "'dgf' AS record_source",
        )
    )
