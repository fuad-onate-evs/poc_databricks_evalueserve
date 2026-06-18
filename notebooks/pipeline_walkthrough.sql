-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 🛠️ Pipeline walkthrough — sources → bronze → silver → gold
-- MAGIC End-to-end explanation of the renewable-energy medallion: **where the data comes from**, how it
-- MAGIC lands in **bronze**, is modeled in **silver**, aggregated in **gold**, and **orchestrated**.
-- MAGIC Each section has the **actual transformation code** + a **live query** against the `dev_fuad_onate` sandbox.
-- MAGIC
-- MAGIC > ⚠️ Sandbox data is **synthetic** (random sample). The pipeline *shape* is real; the *numbers* aren't.
-- MAGIC
-- MAGIC **Flow**
-- MAGIC ```
-- MAGIC  CSV files (UC Volume)            DLT transforms
-- MAGIC  /Volumes/.../lookup/...  ──Auto Loader──▶  BRONZE ──CDF──▶ SILVER ──agg──▶ GOLD ──▶ dashboards
-- MAGIC  (solar/eólica: coor/real/reducc.)  raw+lineage    Data Vault    business metrics
-- MAGIC ```
-- MAGIC **Contents:** 1) Sources · 2) Bronze · 3) Silver · 4) Gold · 5) Conglomerate · 6) End-to-end trace · 7) Orchestration & scheduling · 8) Observability

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## 1. Where the data comes from
-- MAGIC Two domains, each fed by **CSV files dropped into Unity Catalog Volumes** (`/Volumes/<catalog>/<schema>/lookup/…`):
-- MAGIC
-- MAGIC | Domain | Source | Format | Flavors |
-- MAGIC |---|---|---|---|
-- MAGIC | **Resource** (plant-level) | Chile grid operator **CEN / Coordinador** — solar & eólica generation | `;`-separated, Spanish decimals (comma) | `coor` (programmed) · `real` (actual) · `reducciones` (curtailment) |
-- MAGIC | **Conglomerate** (country-level) | OWID-style renewable statistics | `,`-separated | hydropower, solar-PV capacity, production/consumption, electricity share |
-- MAGIC
-- MAGIC > 🔭 **Future (card #56):** a poller will pull **U. de Chile / DGF** meteorological data (or `api.minenergia.cl`)
-- MAGIC > and land it the same way (→ Volume → Auto Loader). See `docs/card-56-bronze-landing-design.md`.

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## 2. Ingestion → BRONZE  (Auto Loader)
-- MAGIC Each bronze table is a **DLT streaming table** that reads new CSVs from a Volume with **Auto Loader**
-- MAGIC (`cloudFiles`), keeping `_metadata` for lineage. Schema evolves automatically.
-- MAGIC ```python
-- MAGIC # src/renewable_energy_chile/transformations/bronze_resource.py  (representative)
-- MAGIC @dlt.table(table_properties={"quality": "bronze"})
-- MAGIC def bronze_real_solar():
-- MAGIC     return (spark.readStream.format("cloudFiles")
-- MAGIC         .option("cloudFiles.format", "csv")
-- MAGIC         .option("cloudFiles.inferColumnTypes", "true")
-- MAGIC         .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
-- MAGIC         .option("header", "true").option("sep", ";")          # Spanish CSV: ';' separated
-- MAGIC         .load(f"/Volumes/{catalog}/{bronze_schema}/lookup/solar/real/")
-- MAGIC         .selectExpr("*",
-- MAGIC             "_metadata.file_path           AS file_path",      # provenance
-- MAGIC             "_metadata.file_modification_time AS modification_date",
-- MAGIC             "'solar' AS resource"))
-- MAGIC ```
-- MAGIC The result is the raw rows **plus** `file_path` (which CSV) + `modification_date` (when loaded).

-- COMMAND ----------
-- Live: raw bronze rows (note the original Spanish 'valor' like 442,9)
SELECT Nombre, Fecha, Hora, valor, resource, file_path
FROM workspace.dev_fuad_onate_renewable_bronze_energy_chile.bronze_real_solar
ORDER BY Nombre, Fecha, Hora
LIMIT 12;

-- COMMAND ----------
-- Live: provenance — which source file each batch came from, how many rows, when loaded
SELECT file_path, resource, COUNT(*) AS rows, MAX(modification_date) AS loaded
FROM workspace.dev_fuad_onate_renewable_bronze_energy_chile.bronze_real_solar
GROUP BY file_path, resource;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## 3. BRONZE → SILVER  (Data Vault + quality)
-- MAGIC Silver reads bronze via the **Change Data Feed** (`readChangeFeed`), cleans values
-- MAGIC (comma→dot, normalize names), applies **DLT expectations**, and upserts into **Data-Vault hubs**
-- MAGIC with **`apply_changes` (SCD Type 1)**.
-- MAGIC ```python
-- MAGIC # src/renewable_energy_chile/transformations/silver_resource.py
-- MAGIC @dlt.view
-- MAGIC @dlt.expect_all_or_drop(rules)                 # data-quality gate
-- MAGIC def silver_hub_plant_combined():
-- MAGIC     def get_source(table_path):
-- MAGIC         return (spark.readStream.option("readChangeFeed", "true").table(table_path)  # CDF
-- MAGIC             .select("Nombre","Fecha","Hora","valor","file_path","resource")
-- MAGIC             .withColumns({
-- MAGIC                 "hash_key":      sha2(col("Nombre"), 256),
-- MAGIC                 "plant_name":    string_transformation.special_characters("Nombre"),
-- MAGIC                 "value_date":    to_timestamp(concat(date_format(col("Fecha"),"yyyy-MM-dd"),
-- MAGIC                                     lit(" "), lpad(col("Hora"),2,"0"), lit(":00:00.0"))),
-- MAGIC                 "value":         string_transformation.change_comma_to_dot("valor"),  # 442,9 → 442.9
-- MAGIC                 "load_date":     current_timestamp(),
-- MAGIC                 "record_source": col("file_path")}))
-- MAGIC     return get_source(f"{bronze_schema}.bronze_real_solar").union(
-- MAGIC            get_source(f"{bronze_schema}.bronze_real_eolic"))
-- MAGIC
-- MAGIC dlt.create_streaming_table(f"{silver_schema}.silver_hub_plant")
-- MAGIC dlt.apply_changes(                              # SCD-1 upsert (the "hub")
-- MAGIC     target=f"{silver_schema}.silver_hub_plant", source="silver_hub_plant_combined",
-- MAGIC     keys=["hash_key","plant_name","record_source","value_date"],
-- MAGIC     sequence_by="load_date", stored_as_scd_type=1)
-- MAGIC ```
-- MAGIC > 🐞 The **coordinated** hub uses `keys=["hash_key"]` only (PR #21) → it collapses the hourly series.
-- MAGIC > That's why `coordinated_value` is mostly NULL downstream. The `silver_sat_measure` view joins the
-- MAGIC > three hubs (plant / reductions / coordinated) into one wide table.

-- COMMAND ----------
-- Live: silver hub — normalized plant + parsed value_date + numeric value
SELECT plant_name, value_date, value, record_source
FROM workspace.dev_fuad_onate_renewable_silver_energy_chile.silver_hub_plant
ORDER BY plant_name, value_date
LIMIT 12;

-- COMMAND ----------
-- Live: silver consolidated (note coordinated_value mostly NULL = the PR #21 fingerprint)
SELECT real_name, coordinated_name, record_date, plant_value, coordinated_value
FROM workspace.dev_fuad_onate_renewable_silver_energy_chile.silver_sat_measure
ORDER BY record_date
LIMIT 12;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## 4. SILVER → GOLD  (business metrics)
-- MAGIC Gold aggregates the consolidated silver into materialized views (daily / weekly / monthly averages
-- MAGIC and the **real − coordinated** gap).
-- MAGIC ```python
-- MAGIC # src/renewable_energy_chile/transformations/gold_resource.py
-- MAGIC @dlt.table(name=f"{gold_schema}.gold_daily_measure")
-- MAGIC def gold_daily_measure():
-- MAGIC     return (dlt.read(f"{silver_schema}.silver_sat_measure")
-- MAGIC         .groupBy("hash_measure_key", "record_date")
-- MAGIC         .agg(coalesce(avg("plant_value"),       lit(0.0)).alias("avg_real"),
-- MAGIC              coalesce(avg("reduction_value"),   lit(0.0)).alias("avg_reductions"),
-- MAGIC              coalesce(avg("coordinated_value"), lit(0.0)).alias("avg_coordinated"))   # coalesce→0 masks NULLs
-- MAGIC         .withColumn("diff_real_coordinated",
-- MAGIC                     coalesce(col("avg_real") - col("avg_coordinated"), lit(0.0))))
-- MAGIC ```

-- COMMAND ----------
-- Live: gold daily metrics (avg_coordinated ≈ 0 because of the upstream coalesce + SCD bug)
SELECT record_date,
       ROUND(avg_real, 1)              AS avg_real,
       ROUND(avg_coordinated, 1)       AS avg_coordinated,
       ROUND(diff_real_coordinated, 1) AS diff_real_coordinated
FROM workspace.dev_fuad_onate_renewable_gold_energy_chile.gold_daily_measure
ORDER BY record_date;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## 5. Conglomerate domain (parallel medallion)
-- MAGIC Same bronze→silver→gold pattern on **country-level** data (`Entity` × `Year`): bronze CSVs →
-- MAGIC `silver_dim_country` + fact tables → gold year-over-year increases and **Chile vs LATAM / World**.

-- COMMAND ----------
-- Live: conglomerate gold — Chile vs LATAM (South America) vs World
SELECT year, ROUND(chile_vs_latam, 2) AS chile_vs_latam, ROUND(chile_vs_world, 2) AS chile_vs_world
FROM workspace.dev_fuad_onate_conglomerate_gold_energy.gold_different_renewable_again_chile
ORDER BY year;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## 6. End-to-end trace — does data flow through every layer?
-- MAGIC One glance: row volume narrowing/aggregating bronze → silver → gold.

-- COMMAND ----------
SELECT 'bronze · real_solar'        AS step, COUNT(*) AS rows FROM workspace.dev_fuad_onate_renewable_bronze_energy_chile.bronze_real_solar
UNION ALL SELECT 'silver · hub_plant',         COUNT(*) FROM workspace.dev_fuad_onate_renewable_silver_energy_chile.silver_hub_plant
UNION ALL SELECT 'silver · sat_measure',       COUNT(*) FROM workspace.dev_fuad_onate_renewable_silver_energy_chile.silver_sat_measure
UNION ALL SELECT 'gold · daily_measure',       COUNT(*) FROM workspace.dev_fuad_onate_renewable_gold_energy_chile.gold_daily_measure
ORDER BY step;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## 7. Orchestration & scheduling — **do we run hourly?**
-- MAGIC **No — currently the pipelines run DAILY.** A single Databricks **Job** (`full_execution`) triggers the
-- MAGIC two **serverless DLT** pipelines (conglomerate → resource, sequentially):
-- MAGIC ```yaml
-- MAGIC # resources/jobs/full_execution.job.yml
-- MAGIC jobs:
-- MAGIC   full_execution:
-- MAGIC     trigger:
-- MAGIC       periodic: { interval: 1, unit: DAYS }     # ← DAILY (not hourly)
-- MAGIC     tasks:
-- MAGIC       - task_key: pipeline_conglomerate
-- MAGIC         pipeline_task: { pipeline_id: ${resources.pipelines.renewable_conglomerate.id} }
-- MAGIC       - task_key: pipeline_energy_chile
-- MAGIC         depends_on: [{ task_key: pipeline_conglomerate }]    # sequential (could be parallel — see PR #20)
-- MAGIC         pipeline_task: { pipeline_id: ${resources.pipelines.renewable_energy_chile.id} }
-- MAGIC ```
-- MAGIC The DLT pipelines are **triggered** (run-to-completion then stop), not **continuous** — so each run is
-- MAGIC cheap and there's no always-on compute.
-- MAGIC
-- MAGIC ### Why hourly is on the roadmap (card #56)
-- MAGIC The DGF discovery set the target frequency to **hourly**. To move from daily → hourly you change the
-- MAGIC **job trigger**, no pipeline-code change:
-- MAGIC ```yaml
-- MAGIC     trigger:
-- MAGIC       periodic: { interval: 1, unit: HOURS }    # hourly
-- MAGIC     # or a cron:  schedule: { quartz_cron_expression: "0 0 * * * ?", timezone_id: "America/Santiago" }
-- MAGIC ```
-- MAGIC Combined with **Auto Loader + `Trigger.AvailableNow`** this gives hourly freshness with **no always-on cost**
-- MAGIC (the recommendation in `docs/card-56-bronze-landing-design.md`). Until that's deployed, runs are **daily**.

-- COMMAND ----------
-- Live: actual run history (DLT event log) — real cadence & last status, converted UTC → Santiago time
SELECT date_format(from_utc_timestamp(timestamp, 'America/Santiago'), 'yyyy-MM-dd HH:mm') AS run_time_santiago,
       details:update_progress.state::string      AS state
FROM workspace.dev_fuad_onate_renewable_bronze_energy_chile.`event_log_9bcfc32a_19f0_4327_b5bc_e8ee1cc4a3a4`
WHERE event_type = 'update_progress'
  AND details:update_progress.state IN ('COMPLETED','FAILED','CANCELED')
ORDER BY timestamp DESC
LIMIT 10;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## 8. Observability & references
-- MAGIC - **Dashboards:** Medallion Health · End-to-End (ingestion → runs → DQ → gold metrics).
-- MAGIC - **Lineage:** Catalog Explorer → Lineage tab (UC tracks bronze→silver→gold automatically).
-- MAGIC - **Data dictionary / governance:** the `data_catalog` notebook · `docs/data-governance.md`.
-- MAGIC - **Glossary:** `docs/glossary.md` · the `glossary` notebook.
-- MAGIC - **Source code:** `src/<domain>/transformations/{bronze,silver,gold}_*.py` · `resources/pipeline/*.yml` · `resources/jobs/*.yml`.
