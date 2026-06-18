# Databricks notebook source
# COMMAND ----------
import sys
from pyspark import pipelines as dp
# COMMAND ----------
sys.path.append('../../.')
from helper import algebraic_operations
# COMMAND ----------
silver_schema = gold_schema = str()
silver_schema = spark.conf.get("silver_schema")
gold_schema = spark.conf.get("gold_schema")

#TODO create a file that have all of the dictionaries
dict_table_name = {'silver_dim_country': f'{silver_schema}.silver_dim_country'
                 , 'silver_fact_energy_consumption': f'{silver_schema}.silver_fact_energy_consumption'
                 , 'silver_fact_energy_production': f'{silver_schema}.silver_fact_energy_production'
                 , 'gold_increase_consumption': f'{gold_schema}.gold_increase_consumption'
                 , 'gold_increase_production': f'{gold_schema}.gold_increase_production'
                 , 'gold_different_chile': f'{gold_schema}.gold_different_renewable_again_chile'}

# COMMAND ----------
table_schema = {
                'gold_increase_consumption':f"""
                    hash_key STRING NOT NULL COMMENT 'SHA-256 country key (FK to silver_dim_country).' PRIMARY KEY,
                    year SMALLINT COMMENT 'Calendar year.',
                    increase_electricity_from_hydro DOUBLE COMMENT 'Year-over-year increase in electricity from hydro.',
                    increase_geo_biomass_other DOUBLE COMMENT 'Year-over-year increase in geothermal/biomass/other generation.',
                    increase_solar_generation DOUBLE COMMENT 'Year-over-year increase in solar generation.',
                    increase_wind_generation DOUBLE COMMENT 'Year-over-year increase in wind generation.',
                    increase_hydro_generation DOUBLE COMMENT 'Year-over-year increase in hydropower generation.',
                    CONSTRAINT fk_hash_key_gold_consumption FOREIGN KEY (hash_key) REFERENCES {dict_table_name['silver_dim_country']}(hash_key)
                """,
                'gold_increase_production': f"""
                    hash_key STRING NOT NULL COMMENT 'SHA-256 country key (FK to silver_dim_country).' PRIMARY KEY,
                    year SMALLINT COMMENT 'Calendar year.',
                    increase_solar_capacity DOUBLE COMMENT 'Year-over-year increase in installed solar PV capacity.',
                    increase_electricity_from_wind DOUBLE COMMENT 'Year-over-year increase in electricity from wind.',
                    increase_electricity_from_hydro DOUBLE COMMENT 'Year-over-year increase in electricity from hydro.',
                    increase_electricity_from_solar DOUBLE COMMENT 'Year-over-year increase in electricity from solar.',
                    increase_other_renewables_including_bioenergy DOUBLE COMMENT 'Year-over-year increase in other renewables including bioenergy.',
                    increase_renewables DOUBLE COMMENT 'Year-over-year increase in total renewables.',
                    CONSTRAINT fk_hash_key_gold_production FOREIGN KEY (hash_key) REFERENCES {dict_table_name['silver_dim_country']}(hash_key)
                """,
                'gold_different_chile': """
                    year SMALLINT COMMENT 'Calendar year.',
                    chile_vs_latam DOUBLE COMMENT 'Chile renewables minus LATAM (South America).',
                    chile_vs_world DOUBLE COMMENT 'Chile renewables minus World.'
                """}
# COMMAND ----------
@dp.materialized_view(name = dict_table_name['gold_increase_consumption'],
                      schema = table_schema['gold_increase_consumption'])
def gold_increase_consumption():
    sdf_silver_fact_energy_consumption = spark.table(dict_table_name['silver_fact_energy_consumption'])
    class_algebaric_operation = algebraic_operations.Algebraic_Operation('hash_key', 'year')

    sdf_energy_consumption = sdf_silver_fact_energy_consumption\
        .withColumn('increase_electricity_from_hydro', class_algebaric_operation.distance_row('Electricity_from_hydro'))\
        .withColumn('increase_geo_biomass_other', class_algebaric_operation.distance_row('geo_biomass_other'))\
        .withColumn('increase_solar_generation', class_algebaric_operation.distance_row('solar_generation'))\
        .withColumn('increase_wind_generation', class_algebaric_operation.distance_row('wind_generation'))\
        .withColumn('increase_hydro_generation', class_algebaric_operation.distance_row('hydro_generation'))

    sdf_output_gold_consumption = sdf_energy_consumption.select('hash_key', 'year', 'increase_electricity_from_hydro', 'increase_geo_biomass_other',
                                                                'increase_solar_generation', 'increase_wind_generation', 'increase_hydro_generation')
    return sdf_output_gold_consumption

# COMMAND ----------
@dp.materialized_view(name = dict_table_name['gold_increase_production'],
                      schema = table_schema['gold_increase_production'])
def gold_increase_production():
    sdf_silver_fact_energy_consumption = spark.table(dict_table_name['silver_fact_energy_production'])
    class_algebaric_operation = algebraic_operations.Algebraic_Operation('hash_key', 'year')

    sdf_energy_consumption = sdf_silver_fact_energy_consumption\
        .withColumn('increase_solar_capacity', class_algebaric_operation.distance_row('solar_capacity'))\
        .withColumn('increase_electricity_from_wind', class_algebaric_operation.distance_row('electricity_from_wind'))\
        .withColumn('increase_electricity_from_hydro', class_algebaric_operation.distance_row('electricity_from_hydro'))\
        .withColumn('increase_electricity_from_solar', class_algebaric_operation.distance_row('electricity_from_solar'))\
        .withColumn('increase_other_renewables_including_bioenergy', class_algebaric_operation.distance_row('other_renewables_including_bioenergy'))\
        .withColumn('increase_renewables', class_algebaric_operation.distance_row('renewables'))
    
    sdf_output_gold_consumption = sdf_energy_consumption.select('hash_key', 'year', 'increase_solar_capacity', 'increase_electricity_from_wind'
                                                                , 'increase_electricity_from_hydro', 'increase_electricity_from_solar'
                                                                , 'increase_other_renewables_including_bioenergy', 'increase_renewables')
    return sdf_output_gold_consumption           

# COMMAND ----------
@dp.materialized_view(name = dict_table_name['gold_different_chile'],
                      schema = table_schema['gold_different_chile'])
def gold_different_chile():

    def get_expecific_country(sdf, country_hash_key: str, alias: str):
        return sdf.where(f"hash_key = '{country_hash_key}'").select('hash_key', 'year', sdf.renewables.alias(alias))
    
    sdf_silver_fact_energy_production = spark.table(dict_table_name['silver_fact_energy_production'])

    sdf_renewable_chile = get_expecific_country(sdf_silver_fact_energy_production, 'ae34a7cc973ea290192f3a87f84e5e638a5f73b9', 'renewables_chile')
    sdf_renewable_latam = get_expecific_country(sdf_silver_fact_energy_production, '123c90e447d63fedfb2c19934dfb16d7d0d7fe76', 'renewables_latam')
    sdf_renewable_world = get_expecific_country(sdf_silver_fact_energy_production, '7c211433f02071597741e6ff5a8ea34789abbf43', 'renewables_world')

    sdf_renewable_consolidate = sdf_renewable_chile.join(sdf_renewable_latam, on=['year'], how='inner')\
                                .join(sdf_renewable_world, on=['year'], how='inner')\
                                .select('year'
                                        , (sdf_renewable_chile.renewables_chile - sdf_renewable_latam.renewables_latam).alias('chile_vs_latam')
                                        , (sdf_renewable_chile.renewables_chile - sdf_renewable_world.renewables_world).alias('chile_vs_world'))
    return sdf_renewable_consolidate
                            