# Databricks notebook source
# COMMAND ----------
import dlt
from pyspark.sql.functions import *

# COMMAND ----------

env: str = spark.conf.get("catalog")
schema : str = spark.conf.get("bronze_schema")
# COMMAND ----------

# Column-level data dictionary for the raw bronze tables. These mirror the
# Auto Loader-inferred schema (same column order + types) and add COMMENTs so
# every field is documented in Catalog Explorer.
# NOTE: pinning the table schema makes a genuinely NEW source column land in
# _rescued_data rather than being auto-added as a new table column — a deliberate
# trade of stability over auto-evolution. Re-pin this schema if the upstream CSV
# shape changes (validate against a DESCRIBE of the inferred schema).
_BRONZE_TAIL = (
    "valor STRING COMMENT 'Measured generation value (raw; Spanish decimal with a comma, e.g. 442,9).', "
    "_rescued_data STRING COMMENT 'Auto Loader rescued data: source values that did not match the inferred schema.', "
    "modification_date TIMESTAMP COMMENT 'Source file modification time (_metadata.file_modification_time) — ingestion freshness.', "
    "file_path STRING COMMENT 'Source CSV path (_metadata.file_path) — record provenance / lineage.', "
    "resource STRING COMMENT 'Energy type tag added at bronze: solar or eolic.'"
)
# real-generation tables carry an explicit Hora column and a DATE Fecha.
SCHEMA_REAL = (
    "Nombre STRING COMMENT 'Plant name (raw, from the source CSV).', "
    "Fecha DATE COMMENT 'Measurement date (raw, from the source CSV).', "
    "Hora INT COMMENT 'Hour of day 0-23 (raw; real-generation tables only).', "
    + _BRONZE_TAIL
)
# coordinated + reductions tables carry a TIMESTAMP Fecha and no Hora.
SCHEMA_COOR_RED = (
    "Nombre STRING COMMENT 'Plant name (raw, from the source CSV).', "
    "Fecha TIMESTAMP COMMENT 'Measurement timestamp (raw, from the source CSV).', "
    + _BRONZE_TAIL
)
# COMMAND ----------


@dlt.table(
    # name="bronze_solar_coordinados",
    table_properties={"layer": "bronze"},
    comment="Ingesting raw CSV files into Unity Catalog using Auto Loader",
    schema=SCHEMA_COOR_RED,
)
def bronze_coordinated_solar():

    source_csv_path = f"/Volumes/{env}/{schema}/lookup/solar/coor/"

    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        # .option("cloudFiles.schemaHints", "valor NUMERIC, fecha DATE")
        .option("header", "true")  # Standard CSV option
        .option("sep", ";")
        .load(source_csv_path)
        # .selectExpr("valor", "nombre", "fecha","_metadata.file_modification_time", "_metadata.file_path")
        .selectExpr(
            "*",
            "_metadata.file_modification_time AS modification_date",
            "_metadata.file_path as file_path",
            "'solar' as resource",
        )
    )


# COMMAND ----------


@dlt.table(
    # name="bronze_solar_real",
    table_properties={"layer": "bronze"},
    comment="Ingesting raw CSV files into Unity Catalog using Auto Loader",
    schema=SCHEMA_REAL,
)
def bronze_real_solar():

    source_csv_path = f"/Volumes/{env}/{schema}/lookup/solar/real/"

    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")  # Auto Loader infers types
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("header", "true")  # Standard CSV option
        .option("sep", ";")
        .load(source_csv_path)
        .selectExpr(
            "*",
            "_metadata.file_modification_time as modification_date",
            "_metadata.file_path as file_path",
            "'solar' as resource",
        )
    )


# COMMAND ----------


@dlt.table(
    # name="bronze_solar_reducciones",
    table_properties={"layer": "bronze"},
    comment="Ingesting raw CSV files into Unity Catalog using Auto Loader",
    schema=SCHEMA_COOR_RED,
)
def bronze_reductions_preliminary_solar():

    source_csv_path = f"/Volumes/{env}/{schema}/lookup/solar/reducciones/"

    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")  # Auto Loader infers types
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("header", "true")  # Standard CSV option
        .option("sep", ";")
        .load(source_csv_path)
        .selectExpr(
            "*",
            "_metadata.file_modification_time as modification_date",
            "_metadata.file_path as file_path",
            "'solar' as resource",
        )
    )


# COMMAND ----------


@dlt.table(
    # name="bronze_coordinated_eolic",
    table_properties={"layer": "bronze"},
    comment="Ingesting raw CSV files into Unity Catalog using Auto Loader",
    schema=SCHEMA_COOR_RED,
)
def bronze_coordinated_eolic():

    source_csv_path = f"/Volumes/{env}/{schema}/lookup/eolic/coor/"

    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        # .option("cloudFiles.schemaHints", "valor NUMERIC, fecha DATE")
        .option("header", "true")  # Standard CSV option
        .option("sep", ";")
        .load(source_csv_path)
        # .selectExpr("valor", "nombre", "fecha","_metadata.file_modification_time", "_metadata.file_path")
        .selectExpr(
            "*",
            "_metadata.file_modification_time AS modification_date",
            "_metadata.file_path as file_path",
            "'eolic' as resource",
        )
    )


# COMMAND ----------


@dlt.table(
    # name="bronze_eolic_real",
    table_properties={"layer": "bronze"},
    comment="Ingesting raw CSV files into Unity Catalog using Auto Loader",
    schema=SCHEMA_REAL,
)
def bronze_real_eolic():

    source_csv_path = f"/Volumes/{env}/{schema}/lookup/eolic/real/"

    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        # .option("cloudFiles.schemaHints", "valor NUMERIC, fecha DATE")
        .option("header", "true")  # Standard CSV option
        .option("sep", ";")
        .load(source_csv_path)
        # .selectExpr("valor", "nombre", "fecha","_metadata.file_modification_time", "_metadata.file_path")
        .selectExpr(
            "*",
            "_metadata.file_modification_time AS modification_date",
            "_metadata.file_path as file_path",
            "'eolic' as resource",
        )
    )


# COMMAND ----------


@dlt.table(
    # name="bronze_reductions_preliminary_eolic",
    table_properties={"layer": "bronze"},
    comment="Ingesting raw CSV files into Unity Catalog using Auto Loader",
    schema=SCHEMA_COOR_RED,
)
def bronze_reductions_preliminary_eolic():

    source_csv_path = f"/Volumes/{env}/{schema}/lookup/eolic/reducciones/"

    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        # .option("cloudFiles.schemaHints", "valor NUMERIC, fecha DATE")
        .option("header", "true")  # Standard CSV option
        .option("sep", ";")
        .load(source_csv_path)
        # .selectExpr("valor", "nombre", "fecha","_metadata.file_modification_time", "_metadata.file_path")
        .selectExpr(
            "*",
            "_metadata.file_modification_time AS modification_date",
            "_metadata.file_path as file_path",
            "'eolic' as resource",
        )
    )