#!/usr/bin/env python3
"""Explore the sample-data medallion from VS Code via Databricks Connect.

Uses the serverless compute in the dbc-54b27bae workspace (no warehouse needed).
Run:  .venv/bin/python scripts/explore_sample_data.py
"""
from __future__ import annotations
import os

# Fall back to serverless compute if no cluster/profile compute is set (same as tests/conftest.py).
os.environ.setdefault("DATABRICKS_SERVERLESS_COMPUTE_ID", "auto")

from databricks.connect import DatabricksSession

CATALOG = "workspace"
TABLES = [
    # (label, fully-qualified table)
    ("Resource · GOLD daily", f"{CATALOG}.dev_fuad_onate_renewable_gold_energy_chile.gold_daily_measure"),
    ("Resource · SILVER sat", f"{CATALOG}.dev_fuad_onate_renewable_silver_energy_chile.silver_sat_measure"),
    ("Resource · BRONZE real solar", f"{CATALOG}.dev_fuad_onate_renewable_bronze_energy_chile.bronze_real_solar"),
    ("Conglomerate · GOLD Chile vs LATAM/World", f"{CATALOG}.dev_fuad_onate_conglomerate_gold_energy.gold_different_renewable_again_chile"),
    ("Conglomerate · SILVER dim_country", f"{CATALOG}.dev_fuad_onate_conglomerate_silver_energy_chile.silver_dim_country"),
]


def main() -> None:
    spark = DatabricksSession.builder.getOrCreate()
    for label, table in TABLES:
        print(f"\n=== {label}  ({table}) ===")
        df = spark.table(table)
        print(f"rows: {df.count()}")
        df.show(10, truncate=False)


if __name__ == "__main__":
    main()
