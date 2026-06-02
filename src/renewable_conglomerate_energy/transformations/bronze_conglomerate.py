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
@dp.table(
    table_properties={"quality": "bronze"},
    cluster_by=['Entity', 'Year']
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
    cluster_by=['Entity', 'Year']
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
    cluster_by=['Entity', 'Year']
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
    cluster_by=['Entity', 'Year']
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
    cluster_by=['Entity', 'Year']
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