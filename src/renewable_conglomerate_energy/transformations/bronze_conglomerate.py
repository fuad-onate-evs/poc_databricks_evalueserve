# Databricks notebook source
# COMMAND ----------
from pyspark import pipelines as dp
from pyspark.sql.functions import *

# COMMAND ----------
env = schema = str()
env = spark.conf.get("catalog")
schema = spark.conf.get("bronze_schema")

raw_path = {"hydropower_consumption": f"/Volumes/{env}/{schema}/lookup/hydropower_consumption/",
            "installed_solar": f"/Volumes/{env}/{schema}/lookup/installed_solar/",
            "renewable_energy_consumption": f"/Volumes/{env}/{schema}/lookup/renewable_energy_consumption/",
            "modern_renewable_prod": f"/Volumes/{env}/{schema}/lookup/modern_renewable_prod/",
            "share_electricity_renewable": f"/Volumes/{env}/{schema}/lookup/share_electricity_renewable/"}

# COMMAND ----------

# Column-level data dictionary for the conglomerate raw bronze tables (Our World
# in Data CSVs). These mirror the Auto Loader-inferred schema (same column order +
# types) and add COMMENTs, documenting every field in Catalog Explorer.
# NOTE: pinning the table schema makes a genuinely NEW source column land in
# _rescued_data rather than being auto-added — a deliberate stability trade-off.
_CONG_TAIL = (
    "_rescued_data STRING COMMENT 'Auto Loader rescued data: source values that did not match the inferred schema.', "
    "modification_date TIMESTAMP COMMENT 'Source file modification time (_metadata.file_modification_time) — ingestion freshness.', "
    "file_path STRING COMMENT 'Source CSV path (_metadata.file_path) — record provenance / lineage.'"
)
_CONG_HEAD = (
    "Entity STRING COMMENT 'Country or region name (Our World in Data Entity).', "
    "Year INT COMMENT 'Calendar year.', "
)
SCHEMA_HYDRO_CONS = _CONG_HEAD + "Hydro_Generation DOUBLE COMMENT 'Hydropower generation.', " + _CONG_TAIL
SCHEMA_SOLAR_CAP = _CONG_HEAD + "Solar_Capacity DOUBLE COMMENT 'Installed solar PV capacity.', " + _CONG_TAIL
SCHEMA_MODERN_CONS = _CONG_HEAD + (
    "Electricity_from_hydro DOUBLE COMMENT 'Electricity generated from hydro.', "
    "Geo_Biomass_Other DOUBLE COMMENT 'Generation from geothermal, biomass and other sources.', "
    "Solar_Generation DOUBLE COMMENT 'Solar generation.', "
    "Wind_Generation DOUBLE COMMENT 'Wind generation.', "
) + _CONG_TAIL
SCHEMA_MODERN_PROD = _CONG_HEAD + (
    "Electricity_from_wind DOUBLE COMMENT 'Electricity generated from wind.', "
    "Electricity_from_hydro DOUBLE COMMENT 'Electricity generated from hydro.', "
    "Electricity_from_solar DOUBLE COMMENT 'Electricity generated from solar.', "
    "Other_renewables_including_bioenergy DOUBLE COMMENT 'Other renewables including bioenergy.', "
) + _CONG_TAIL
SCHEMA_SHARE = _CONG_HEAD + "Renewables DOUBLE COMMENT 'Renewables share of electricity.', " + _CONG_TAIL

# COMMAND ----------
@dp.table(
    table_properties={"quality": "bronze"},
    cluster_by=['Entity', 'Year'],
    schema=SCHEMA_HYDRO_CONS,
)
def bronze_hydropower_consumption():

    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumnsWithTypeWidening")
        .option("header", "true")
        .option("sep", ",")
        .load(raw_path['hydropower_consumption'])
        .selectExpr(
            "*",
            "_metadata.file_modification_time AS modification_date",
            "_metadata.file_path as file_path",
        )
    )

# COMMAND ----------
@dp.table(
    table_properties={"quality": "bronze"},
    cluster_by=['Entity', 'Year'],
    schema=SCHEMA_SOLAR_CAP,
)
def bronze_installed_solar_PV_capacity():

    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumnsWithTypeWidening")
        .option("header", "true")
        .option("sep", ",")
        .load(raw_path['installed_solar'])
        .selectExpr(
            "*",
            "_metadata.file_modification_time AS modification_date",
            "_metadata.file_path as file_path",
        )
    )

# COMMAND ----------
@dp.table(
    table_properties={"quality": "bronze"},
    cluster_by=['Entity', 'Year'],
    schema=SCHEMA_MODERN_CONS,
)
def bronze_modern_renewable_energy_consumption():

    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumnsWithTypeWidening")
        .option("header", "true")
        .option("sep", ",")
        .load(raw_path['renewable_energy_consumption'])
        .selectExpr(
            "*",
            "_metadata.file_modification_time AS modification_date",
            "_metadata.file_path as file_path",
        )
    )

# COMMAND ----------
@dp.table(
    table_properties={"quality": "bronze"},
    cluster_by=['Entity', 'Year'],
    schema=SCHEMA_MODERN_PROD,
)
def bronze_modern_renewable_prod():

    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumnsWithTypeWidening")
        .option("header", "true")
        .option("sep", ",")
        .load(raw_path['modern_renewable_prod'])
        .selectExpr(
            "*",
            "_metadata.file_modification_time AS modification_date",
            "_metadata.file_path as file_path",
        )
    )

# COMMAND ----------
@dp.table(
    table_properties={"quality": "bronze"},
    cluster_by=['Entity', 'Year'],
    schema=SCHEMA_SHARE,
)
def bronze_share_electricity_renewable():

    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumnsWithTypeWidening")
        .option("header", "true")
        .option("sep", ",")
        .load(raw_path['share_electricity_renewable'])
        .selectExpr(
            "*",
            "_metadata.file_modification_time AS modification_date",
            "_metadata.file_path as file_path",
        )
    )