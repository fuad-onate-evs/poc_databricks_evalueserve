# Databricks notebook source
# COMMAND ----------
import sys
from pyspark import pipelines as dp
# COMMAND ----------
sys.path.append('../.')
from helper import string_transformation, anonymization

# COMMAND ----------
env = bronze_schema = silver_schema = str()
env = spark.conf.get("catalog")
bronze_schema = spark.conf.get("bronze_schema")
silver_schema = spark.conf.get("silver_schema")

dict_table_name = {'bronze_renewable_prod':f'{bronze_schema}.bronze_modern_renewable_prod'
                 , 'bronze_renewable_energy_consumption': f'{bronze_schema}.bronze_modern_renewable_energy_consumption'
                 , 'bronze_electricity_renewable':f'{bronze_schema}.bronze_share_electricity_renewable'
                 , 'bronze_solar_pv_capacity':f'{bronze_schema}.bronze_installed_solar_pv_capacity'
                 , 'bronze_hydropower_consumption':f'{bronze_schema}.bronze_hydropower_consumption'
                 , 'silver_dim_country': f'{silver_schema}.silver_dim_country'
                 , 'silver_fact_energy_consumption': f'{silver_schema}.silver_fact_energy_consumption'
                 , 'silver_fact_energy_production': f'{silver_schema}.silver_fact_energy_production'}
# COMMAND ----------
@dp.table(name = f"{dict_table_name['silver_dim_country']}",
          table_properties = {
                            "quality":"silver"
                        })
def silver_dim_country():

    def unique_key(table_name: str):
        return dp.read_stream(dict_table_name[table_name]).select(string_transformation.generalize_str('Entity').alias('Entity')).distinct()

    sdf_bronze_reneweable_prod = unique_key('bronze_renewable_prod')
    sdf_bronze_renewable_energy_consumption = unique_key('bronze_renewable_energy_consumption')
    sdf_bronze_electricity_renewable = unique_key('bronze_electricity_renewable')
    sdf_bronze_solar_pv_capacity = unique_key('bronze_solar_pv_capacity')
    sdf_bronze_hydropower_consumption = unique_key('bronze_hydropower_consumption')
    sdf_dim_country = sdf_bronze_reneweable_prod.union(sdf_bronze_renewable_energy_consumption).union(sdf_bronze_electricity_renewable)\
        .union(sdf_bronze_solar_pv_capacity).union(sdf_bronze_hydropower_consumption)\
        .select(anonymization.hash_key('Entity').alias('hash_key'), 'Entity')
    return sdf_dim_country

# COMMAND ----------
#TODO add expectation, schema and clustering
@dp.table(name = f"{dict_table_name['silver_fact_energy_consumption']}",
          table_properties = {
                            "quality":"silver"
                        })
def silver_fact_energy_consumption():

    sdf_bronze_renewable_energy_consumption = dp.read_stream(dict_table_name['bronze_renewable_energy_consumption'])\
        .withColumn('Entity', string_transformation.generalize_str('Entity').alias('Entity'))
    sdf_bronze_hydropower_consumption = dp.read_stream(dict_table_name['bronze_hydropower_consumption'])\
        .withColumn('Entity', string_transformation.generalize_str('Entity').alias('Entity'))
    sdf_silver_dim_country = dp.read_stream(dict_table_name['silver_dim_country'])

    sdf_silver_fact_energy_consumption = sdf_silver_dim_country\
        .join(sdf_bronze_renewable_energy_consumption, on=['Entity'], how='inner')\
        .join(sdf_bronze_hydropower_consumption, on=['Entity', 'Year'], how='inner')\
        .select('hash_key', 'year', anonymization.hash_key('entity', 'year').alias('diff_hash')
            ,'Electricity_from_hydro', 'Geo_Biomass_Other', 'Solar_Generation', 'Wind_Generation', 'Hydro_Generation')
    
    return sdf_silver_fact_energy_consumption

# COMMAND ----------
#TODO add expectation, schema and clustering
@dp.table(name = f"{dict_table_name['silver_fact_energy_production']}",
          table_properties = {
                            "quality":"silver"
                        })
def silver_fact_energy_production():

    sdf_bronze_solar_pv_capacity = dp.read_stream(dict_table_name['bronze_solar_pv_capacity'])\
        .withColumn('Entity', string_transformation.generalize_str('Entity').alias('Entity'))
    sdf_bronze_renewable_prod = dp.read_stream(dict_table_name['bronze_renewable_prod'])\
        .withColumn('Entity', string_transformation.generalize_str('Entity').alias('Entity'))
    sdf_bronze_electricity_renewable = dp.read_stream(dict_table_name['bronze_electricity_renewable'])\
        .withColumn('Entity', string_transformation.generalize_str('Entity').alias('Entity'))
    sdf_silver_dim_country = dp.read_stream(dict_table_name['silver_dim_country'])

    sdf_silver_fact_energy_production = sdf_silver_dim_country\
        .join(sdf_bronze_solar_pv_capacity, on=['Entity'], how='inner')\
        .join(sdf_bronze_renewable_prod, on=['Entity', 'Year'], how='inner')\
        .join(sdf_bronze_electricity_renewable, on=['Entity', 'Year'], how='inner')\
        .select('hash_key', 'year', anonymization.hash_key('entity', 'year').alias('diff_hash')
         ,'Solar_Capacity', 'Electricity_from_wind', 'Electricity_from_hydro'
         , 'Electricity_from_solar', 'Other_renewables_including_bioenergy', 'Renewables')
    
    return sdf_silver_fact_energy_production
    