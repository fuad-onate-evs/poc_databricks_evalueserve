# Databricks notebook source
# Weather streaming solution — SILVER.
# Flatten + type the raw weather payload into one clean row per plant.
# COMMAND ----------
import dlt
from pyspark.sql.functions import col, to_timestamp, row_number
from pyspark.sql.window import Window

bronze_schema: str = spark.conf.get("bronze_schema")
silver_schema: str = spark.conf.get("silver_schema")
# COMMAND ----------


@dlt.table(
    name=f"{silver_schema}.weather_silver",
    comment="Cleaned DGF weather per plant: solar irradiance (GHI/DNI), temperature and "
            "wind speed, typed from the raw VistaRapida payload.",
    table_properties={"layer": "silver", "quality": "silver"},
)
def weather_silver():
    flat = (
        dlt.read(f"{bronze_schema}.weather_bronze")
        .where(col("server_status") == "ok")
        .select(
            col("name").alias("plant"),
            col("resource"),
            col("lat"),
            col("lon"),
            col("data.VR.GHI").alias("ghi"),       # global horizontal irradiance (kWh/m2/day)
            col("data.VR.DNI").alias("dni"),       # direct normal irradiance (kWh/m2/day)
            col("data.VR.TMP").alias("temp_c"),    # mean temperature (C)
            col("data.VR.VEL").alias("wind_ms"),   # mean wind speed (m/s)
            to_timestamp(col("fetched_at_utc")).alias("loaded_at"),
        )
    )
    # Keep only the latest fetch per plant — hourly re-fetches would otherwise accumulate.
    latest = Window.partitionBy("plant", "resource").orderBy(col("loaded_at").desc())
    return flat.withColumn("_rn", row_number().over(latest)).where(col("_rn") == 1).drop("_rn")
