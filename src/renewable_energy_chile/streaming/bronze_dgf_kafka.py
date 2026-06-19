# Databricks notebook source
# Option B — DGF meteorological bronze read DIRECTLY from Kafka (native streaming).
#
# Use this instead of Option A when sub-minute / true streaming is required. The
# producer (scripts/dgf_kafka_producer.py) publishes DGF JSON records to a topic;
# this DLT table reads them natively. Each row = one Kafka record; the raw JSON is
# kept in `payload` with topic/partition/offset lineage (silver parses downstream).
# See docs/card-56-bronze-landing-design.md §2 (Option B) and §3 ("If Kafka is required").
#
# NOT wired into the pipeline yet, and it needs an always-on / continuous pipeline +
# broker credentials. To activate, see src/renewable_energy_chile/streaming/README.md.
# COMMAND ----------
import dlt
from pyspark.sql.functions import col, current_timestamp, lit

bronze_schema: str = spark.conf.get("bronze_schema")

# Pipeline configuration (set in the pipeline's `configuration:` or via a secret scope).
# Defaults point at the local dev broker used for the streaming demo.
def _conf(key, default):
    try:
        return spark.conf.get(key)
    except Exception:
        return default

KAFKA_BOOTSTRAP = _conf("kafka.bootstrap", "localhost:9092")
KAFKA_TOPIC = _conf("kafka.topic", "dgf_met")
# COMMAND ----------


@dlt.table(
    name=f"{bronze_schema}.bronze_dgf_met_kafka",
    comment="Raw DGF met data read directly from Kafka (card #56, landing Option B). One row "
            "per Kafka record; `payload` is the raw JSON, with topic/partition/offset lineage.",
    table_properties={"layer": "bronze", "quality": "bronze", "source": "dgf", "ingest": "kafka"},
)
def bronze_dgf_met_kafka():
    reader = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        # For a secured broker, read credentials from a Databricks secret scope, e.g.:
        #   .option("kafka.security.protocol", "SASL_SSL")
        #   .option("kafka.sasl.mechanism", "PLAIN")
        #   .option("kafka.sasl.jaas.config", dbutils.secrets.get("dgf", "kafka_jaas"))
    )
    return (
        reader.load()
        .select(
            col("value").cast("string").alias("payload"),     # raw JSON message
            col("key").cast("string").alias("message_key"),
            col("topic"),
            col("partition"),
            col("offset"),
            col("timestamp").alias("kafka_timestamp"),         # broker append time
            current_timestamp().alias("ingest_time"),
            lit("dgf").alias("record_source"),
        )
    )
