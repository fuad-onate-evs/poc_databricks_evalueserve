# Databricks notebook source
# COMMAND ----------
import dlt
from expectation import rules
from pyspark.sql.functions import *

# COMMAND ----------
@dlt.view
@dlt.expect_all_or_drop(rules)
def silver_hub_planta_combined():
    # Helper to apply the same transformation to both sources
    def get_source(table_path):
        return (
            spark.readStream.option("readChangeFeed", "true")
            .table(table_path)
            .selectExpr(
                "sha2(`nombre`, 256) as hash_key",
                "regexp_replace(nombre, '\\\ufffd', 'Ñ') as plant_name",  # reemplazar las Ñ por N, dejar nombres en MAYUS,
                "to_timestamp(concat(date(fecha),' ',lpad(cast(hora as string), 2, '0'),':00:00.0')) as value_date",  # Cambiar nombre column a record_date
                "valor as value",
                "current_timestamp() as load_date",
                "file_path as record_source",
                "resource",
            )
        )

    # Union the two streaming sources
    solar_df = get_source("bronze_renewable_energy.bronze_real_solar")
    eolic_df = get_source("bronze_renewable_energy.bronze_real_eolic")

    return solar_df.union(eolic_df)

# COMMAND ----------
dlt.create_streaming_table("silver_renewable_energy.silver_hub_planta")

dlt.apply_changes(
    target="silver_renewable_energy.silver_hub_planta",
    source="silver_hub_planta_combined",
    # Keys define what makes a record "unique" (deduplication keys)
    keys=["hash_key", "plant_name", "record_source", "value_date"],
    # sequence_by ensures that if a duplicate arrives, the latest one wins
    sequence_by="load_date",
    # SCD Type 1 overwrites existing rows with new data (effectively deleting the old dupe)
    stored_as_scd_type=1,
)

@dlt.view
@dlt.expect_all_or_drop(rules)
def silver_hub_reductions_combined():
    # Helper to apply the same transformation to both sources
    def get_source(table_path):
        return (
            spark.readStream.option("readChangeFeed", "true")
            .table(table_path)
            .selectExpr(
                "sha2(`nombre`, 256) as hash_key",
                "regexp_replace(nombre, '\\\ufffd', 'Ñ') as plant_name",
                "to_timestamp(concat(date(fecha),' ',hour(fecha),':00:00.0')) as value_date",
                "valor as value",
                "current_timestamp() as load_date",
                "file_path as record_source",
                "resource",
            )
        )

    # Union the two streaming sources
    eolic_df = get_source("bronze_renewable_energy.bronze_reductions_preliminary_eolic")
    solar_df = get_source("bronze_renewable_energy.bronze_reductions_preliminary_solar")

    return solar_df.union(eolic_df)

# COMMAND ----------
dlt.create_streaming_table("silver_renewable_energy.silver_hub_reductions")

dlt.apply_changes(
    target="silver_renewable_energy.silver_hub_reductions",
    source="silver_hub_reductions_combined",
    # Keys define what makes a record "unique" (deduplication keys)
    keys=["hash_key", "plant_name", "record_source", "value_date"],
    # sequence_by ensures that if a duplicate arrives, the latest one wins
    sequence_by="load_date",
    # SCD Type 1 overwrites existing rows with new data (effectively deleting the old dupe)
    stored_as_scd_type=1,
)


@dlt.view
@dlt.expect_all_or_drop(rules)
def silver_hub_coordinated_combined():
    # Helper to apply the same transformation to both sources
    def get_source(table_path):
        return (
            spark.readStream.option("readChangeFeed", "true")
            .table(table_path)
            .selectExpr(
                "sha2(`nombre`, 256) as hash_key",
                "regexp_replace(nombre, '\\\ufffd', 'Ñ') as plant_name",
                "to_timestamp(concat(date(fecha),' ',hour(fecha),':00:00.0')) as value_date",
                "valor as value",
                "current_timestamp() as load_date",
                "file_path as record_source",
                "resource",
            )
        )

    # Union the two streaming sources
    solar_df = get_source("bronze_renewable_energy.bronze_coordinated_solar")
    eolic_df = get_source("bronze_renewable_energy.bronze_coordinated_eolic")

    return solar_df.union(eolic_df)

# COMMAND ----------
dlt.create_streaming_table("silver_renewable_energy.silver_hub_coordinated")

dlt.apply_changes(
    target="silver_renewable_energy.silver_hub_coordinated",
    source="silver_hub_coordinated_combined",
    # Keys define what makes a record "unique" (deduplication keys)
    keys=["hash_key"],
    # sequence_by ensures that if a duplicate arrives, the latest one wins
    sequence_by="load_date",
    # SCD Type 1 overwrites existing rows with new data (effectively deleting the old dupe)
    stored_as_scd_type=1,
)


@dlt.table(
    name="silver_renewable_energy.silver_link_measure",
    comment="Wide table with individual plant names from 3 sources via Full Outer Join",
)
def silver_link_measure():

    df_c = (
        spark.read.table("silver_renewable_energy.silver_hub_coordinated")
        .select(col("plant_name").alias("coordinated_key"))
        .distinct()
    )

    df_a = (
        spark.read.table("silver_renewable_energy.silver_hub_planta")
        .select(col("plant_name").alias("plant_key"))
        .distinct()
    )

    df_b = (
        spark.read.table("silver_renewable_energy.silver_hub_reductions")
        .select(col("plant_name").alias("reductions_key"))
        .distinct()
    )

    # Perform Full Outer Joins

    joined_df = df_a.join(df_b, df_a.plant_key == df_b.reductions_key, "full").join(
        df_c, (df_a.plant_key == df_c.coordinated_key) | (df_b.reductions_key == df_c.coordinated_key), "full"
    )

    # 5. Add record_date
    return (
        joined_df.withColumn("record_date", current_date())
        .withColumn("hash_key", sha2(concat_ws("||", "coordinated_key", "plant_key", "reductions_key"), 256))
        .distinct()
    )
