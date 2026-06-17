# Databricks notebook source
import sys
import dlt
from expectation import rules
from pyspark.sql.functions import *
# COMMAND ----------
sys.path.append('../../.')
from helper import string_transformation

# COMMAND ----------
bronze_schema : str = spark.conf.get("bronze_schema")
silver_schema : str = spark.conf.get("silver_schema")
# COMMAND ----------
@dlt.view
@dlt.expect_all_or_drop(rules)
def silver_hub_plant_combined():

    def get_source(table_path):
        return (
            spark.readStream.option("readChangeFeed", "true")
            .table(table_path)
            .select(["Nombre", "Fecha", "Hora", "valor", "file_path", "resource"])
            .withColumns({
                "hash_key": sha2(col("Nombre"), 256),
                "plant_name": string_transformation.special_characters("Nombre"),
                "value_date": to_timestamp(concat(date_format(col("Fecha"), "yyyy-MM-dd"), lit(" "), lpad(col("Hora"), 2, "0"), lit(":00:00.0"))),
                "value": string_transformation.change_comma_to_dot("valor"),
                "load_date": current_timestamp(),
                "record_source": col("file_path")
            })
            .select(["hash_key", "plant_name", "value_date", "value", "load_date", "record_source", "resource"])
           
        )


    solar_df = get_source(f"{bronze_schema}.bronze_real_solar")
    eolic_df = get_source(f"{bronze_schema}.bronze_real_eolic")

    return solar_df.union(eolic_df)


dlt.create_streaming_table(f"{silver_schema}.silver_hub_plant")

dlt.apply_changes(
    target=f"{silver_schema}.silver_hub_plant",
    source="silver_hub_plant_combined",
    keys=["hash_key", "plant_name", "record_source", "value_date"],
    sequence_by="load_date",
    stored_as_scd_type=1,
)

# COMMAND ----------
# DBTITLE 1,SILVER_HUB_REDUCTIONS

@dlt.view
@dlt.expect_all_or_drop(rules)
def silver_hub_reductions_combined():
    # Helper to apply the same transformation to both sources
    def get_source(table_path):
        return (
            spark.readStream.option("readChangeFeed", "true")
            .table(table_path)
            .select(["Nombre", "Fecha", "valor", "file_path", "resource"])
            .withColumns({
                "hash_key": sha2(col("Nombre"), 256),
                "plant_name": string_transformation.special_characters("Nombre"),
                "value_date": to_timestamp(concat(date_format(col("Fecha"), "yyyy-MM-dd"), lit(" "), date_format(col("Fecha"), "HH"), lit(":00:00.0"))),
                "value": string_transformation.change_comma_to_dot("valor"),
                "load_date": current_timestamp(),
                "record_source": col("file_path")
            })
            .select(["hash_key", "plant_name", "value_date", "value", "load_date", "record_source", "resource"])
        )

    eolic_df = get_source(f"{bronze_schema}.bronze_reductions_preliminary_eolic")
    solar_df = get_source(f"{bronze_schema}.bronze_reductions_preliminary_solar")

    return solar_df.union(eolic_df)

dlt.create_streaming_table(f"{silver_schema}.silver_hub_reductions")

dlt.apply_changes(
    target=f"{silver_schema}.silver_hub_reductions",
    source="silver_hub_reductions_combined",
    keys=["hash_key", "plant_name", "record_source", "value_date"],
    sequence_by="load_date",
    stored_as_scd_type=1,
)

# COMMAND ----------
# DBTITLE 1,SILVER_HUB_COORDINATED
@dlt.view
@dlt.expect_all_or_drop(rules)
def silver_hub_coordinated_combined():
    def get_source(table_path):
        return (
            spark.readStream.option("readChangeFeed", "true")
            .table(table_path)
            .select(["Nombre", "Fecha", "valor", "file_path", "resource"])
            .withColumns({
                "hash_key": sha2(col("Nombre"), 256),
                "plant_name": string_transformation.special_characters("Nombre"),
                "value_date": to_timestamp(concat(date_format(col("Fecha"), "yyyy-MM-dd"), lit(" "), date_format(col("Fecha"), "HH"), lit(":00:00.0"))),
                "value": string_transformation.change_comma_to_dot("valor"),
                "load_date": current_timestamp(),
                "record_source": col("file_path")
            })
            .select(["hash_key", "plant_name", "value_date", "value", "load_date", "record_source", "resource"])
        )

    solar_df = get_source(f"{bronze_schema}.bronze_coordinated_solar")
    eolic_df = get_source(f"{bronze_schema}.bronze_coordinated_eolic")

    return solar_df.union(eolic_df)

dlt.create_streaming_table(f"{silver_schema}.silver_hub_coordinated")

dlt.apply_changes(
    target=f"{silver_schema}.silver_hub_coordinated",
    source="silver_hub_coordinated_combined",
    keys=["hash_key", "plant_name", "record_source", "value_date"],
    sequence_by="load_date",
    stored_as_scd_type=1,
)

# COMMAND ----------
# DBTITLE 1,SILVER_LINK_MEASURE

@dlt.table(
    name=f"{silver_schema}.silver_link_measure",
    comment="Wide table with individual plant names from 3 sources via Full Outer Join",
)
def silver_link_measure():

    df_c = (
        spark.read.table(f"{silver_schema}.silver_hub_coordinated")
        .select(col("plant_name").alias("coordinated_key"))
        .distinct()
    )

    df_a = (
        spark.read.table(f"{silver_schema}.silver_hub_plant")
        .select(col("plant_name").alias("plant_key"))
        .distinct()
    )

    df_b = (
        spark.read.table(f"{silver_schema}.silver_hub_reductions")
        .select(col("plant_name").alias("reductions_key"))
        .distinct()
    )

    joined_df = df_a.join(df_b, df_a.plant_key == df_b.reductions_key, "full").join(
        df_c, (df_a.plant_key == df_c.coordinated_key) | (df_b.reductions_key == df_c.coordinated_key), "full"
    )

    return (
        joined_df.withColumn("record_date", current_date())
        .withColumn("hash_key", sha2(concat_ws("||", "coordinated_key", "plant_key", "reductions_key"), 256))
        .distinct()
    )

# COMMAND ----------
# DBTITLE 1,SILVER_SAT_MEASURE

@dlt.table(
    name=f"{silver_schema}.silver_sat_measure",
    comment="Consolidated silver hub table joining plants, reductions, and coordinated resources."
)
def silver_sat_measure():
    
    t1 = spark.read.table(f"{silver_schema}.silver_hub_plant")
    t2 = spark.read.table(f"{silver_schema}.silver_hub_reductions")
    t3 = spark.read.table(f"{silver_schema}.silver_hub_coordinated")

    joined_df = t1.alias("t1").join(
        t2.alias("t2"),
        (col("t1.plant_name") == col("t2.plant_name")) & 
        (col("t1.value_date") == col("t2.value_date")),
        how="full_outer"
    )


    final_join = joined_df.join(
        t3.alias("t3"),
        (coalesce(col("t1.plant_name"), col("t2.plant_name")) == col("t3.plant_name")) &
        (coalesce(col("t1.value_date"), col("t2.value_date")) == col("t3.value_date")),
        how="full_outer"
    )

    return final_join \
            .select(
                col("t1.plant_name").alias("real_name"),
                col("t2.plant_name").alias("reduction_name"),
                col("t3.plant_name").alias("coordinated_name"),
                col("t1.value_date").alias("real_date"),
                col("t2.value_date").alias("reduction_date"),
                col("t3.value_date").alias("coordinated_date"),
                col("t1.value").alias("plant_value"),
                col("t2.value").alias("reduction_value"),
                col("t3.value").alias("coordinated_value"),
            ) \
            .withColumns({
                "hash_measure_key": sha2(concat(
                    coalesce(col("real_name"), lit("")),
                    coalesce(col("reduction_name"), lit("")),
                    coalesce(col("coordinated_name"), lit(""))
                    ), 256),
                "diff_key": sha2(concat(
                        coalesce(col("real_name"), lit("")),
                        coalesce(col("reduction_name"), lit("")),
                        coalesce(col("coordinated_name"), lit("")),
                        coalesce(col("real_date"), col("reduction_date"), col("coordinated_date"))
                    ), 256),
                "record_date": coalesce(col("real_date"), col("reduction_date"), col("coordinated_date")),
            }) \
            .select("hash_measure_key", "diff_key", "real_name", "reduction_name", "coordinated_name", "record_date", "plant_value", "reduction_value", "coordinated_value") \
            .distinct()
