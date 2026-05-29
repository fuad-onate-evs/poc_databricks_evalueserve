# Databricks notebook source
# COMMAND ----------
from pyspark import pipelines as dp
import pyspark.sql.functions as F


#TODO check key column generalization, expecific columns, add alias, add hash
@dp.table(name = "silver_conglomerate_energy.silver_fact_energy_consumption",
          table_properties = {
                            "quality":"silver"
                        })
def silver_fact_energy_consumption():
    return (
    dp.read_stream("bronze_modern_renewable_energy_consumption") \
    .join(dp.read_stream("bronze_hydropower_consumption"), on=["Entity", "Year"], how='inner')) \
    .select("bronze_modern_renewable_energy_consumption.Entity",
            "bronze_modern_renewable_energy_consumption.Year",
            "bronze_modern_renewable_energy_consumption.Geo_Biomass_Other",          
            "bronze_modern_renewable_energy_consumption.Solar_Generation",
            "bronze_modern_renewable_energy_consumption.Wind_Generation",
            "bronze_modern_renewable_energy_consumption.Hydro_Generation",
            "bronze_hydropower_consumption.Electricity_from_hydro"
            )