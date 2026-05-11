import dlt
from pyspark.sql.functions import *

# 1. Define the source path (Unity Catalog Volume or External Location)
# Replace with your actual path
#env = 'dev'
#volume = dbutils.widgets.get('catalog')
#volume = spark.conf.get("volume")
env :str = spark.conf.get("catalog")

@dlt.table(
    # name="bronze_solar_coordinados",
    table_properties={"layer": "bronze", "environment": "dev"},
    comment="Ingesting raw CSV files into Unity Catalog using Auto Loader",
)
def bronze_coordinated_solar():

    source_csv_path = f"/Volumes/{env}/bronze_renewable_energy/files_catalog/solar/coor/"

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


@dlt.table(
    # name="bronze_solar_real",
    table_properties={"layer": "bronze", "environment": "dev"},
    comment="Ingesting raw CSV files into Unity Catalog using Auto Loader",
)
def bronze_real_solar():

    source_csv_path = f"/Volumes/{env}/bronze_renewable_energy/files_catalog/solar/real/"

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


@dlt.table(
    # name="bronze_solar_reducciones",
    table_properties={"layer": "bronze", "environment": "dev"},
    comment="Ingesting raw CSV files into Unity Catalog using Auto Loader",
)
def bronze_reductions_preliminary_solar():

    source_csv_path = (
        f"/Volumes/{env}/bronze_renewable_energy/files_catalog/solar/reducciones/"
    )

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


@dlt.table(
    # name="bronze_coordinated_eolic",
    table_properties={"layer": "bronze", "environment": "dev"},
    comment="Ingesting raw CSV files into Unity Catalog using Auto Loader",
)
def bronze_coordinated_eolic():

    source_csv_path = f"/Volumes/{env}/bronze_renewable_energy/files_catalog/eolic/coor/"

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


@dlt.table(
    # name="bronze_eolic_real",
    table_properties={"layer": "bronze", "environment": "dev"},
    comment="Ingesting raw CSV files into Unity Catalog using Auto Loader",
)
def bronze_real_eolic():

    source_csv_path = (
        f"/Volumes/{env}/bronze_renewable_energy/files_catalog/eolic/real/"
    )

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


@dlt.table(
    # name="bronze_reductions_preliminary_eolic",
    table_properties={"layer": "bronze", "environment": "dev"},
    comment="Ingesting raw CSV files into Unity Catalog using Auto Loader",
)
def bronze_reductions_preliminary_eolic():

    source_csv_path = (
        f"/Volumes/{env}/bronze_renewable_energy/files_catalog/eolic/reducciones/"
    )

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
