# Databricks notebook source
# Silver — star schema for the NASA weather domain: dim_plant + fact_weather_hourly.
import dlt
from pyspark.sql.functions import col, month, row_number, sha2, year
from pyspark.sql.window import Window

bronze_schema = spark.conf.get("bronze_schema")
silver_schema = spark.conf.get("silver_schema")

# Data-quality contract (drops rows that fail).
DQ = {
    "valid_coords": "lat BETWEEN -56 AND -17 AND lon BETWEEN -76 AND -66",
    "non_negative_ghi": "ghi >= 0",
    "has_momento": "momento IS NOT NULL",
}


@dlt.table(name=f"{silver_schema}.silver_weather_nasa_dim_plant",
           table_properties={"quality": "silver"},
           comment="Plant dimension: one row per plant with resource and coordinates.")
def silver_weather_nasa_dim_plant():
    return (
        dlt.read(f"{bronze_schema}.bronze_weather_nasa_power")
        .groupBy("plant", "resource")
        .agg({"lat": "avg", "lon": "avg"})
        .selectExpr("sha2(plant, 256) AS plant_key", "plant", "resource",
                    "round(`avg(lat)`, 5) AS lat", "round(`avg(lon)`, 5) AS lon")
    )


@dlt.table(name=f"{silver_schema}.silver_weather_nasa_fact_hourly",
           table_properties={"quality": "silver"},
           comment="Hourly weather fact per plant (FK plant_key); DQ-filtered and deduplicated.")
@dlt.expect_all_or_drop(DQ)
def silver_weather_nasa_fact_hourly():
    w = Window.partitionBy("plant", "momento").orderBy(col("momento"))
    return (
        dlt.read(f"{bronze_schema}.bronze_weather_nasa_power")
        .withColumn("_rn", row_number().over(w))
        .filter(col("_rn") == 1)
        .select(
            sha2(col("plant"), 256).alias("plant_key"),
            "momento", year("momento").alias("anio"), month("momento").alias("mes"),
            "ghi", "temperatura", "humedad", "presion", "agua_caida",
            "viento_10m", "direccion_viento", "viento_50m",
        )
    )
