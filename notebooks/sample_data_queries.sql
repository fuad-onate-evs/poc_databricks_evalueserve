-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Sample data — full medallion explore & validate
-- MAGIC ### catalog `workspace`, sandbox schema prefix `dev_fuad_onate`
-- MAGIC
-- MAGIC This notebook walks the renewable-energy medallion **bronze → silver → gold** for two domains
-- MAGIC (Resource = plant-level Chile, Conglomerate = country-level), and **validates that each layer
-- MAGIC populated** — it does not just dump rows.
-- MAGIC
-- MAGIC > **⚠️ WARNING — SYNTHETIC DATA.** Every value below is **randomly generated** sample data used
-- MAGIC > only to verify that the bronze → silver → gold pipeline shape is correct. These are **NOT real
-- MAGIC > Chilean generation figures** — do not interpret magnitudes, diffs, or country comparisons. The
-- MAGIC > real feed (`api.minenergia.cl` / DGF, Trello card #56) is still pending.
-- MAGIC
-- MAGIC > **🐞 KNOWN ISSUE (PR #21).** `silver_hub_coordinated` collapses the *coordinado* series via an
-- MAGIC > SCD key (~7 rows vs ~42 in `silver_hub_plant`). As a result `silver_sat_measure.coordinated_value`
-- MAGIC > is mostly **NULL** and `gold_daily_measure.avg_coordinated` is mostly **0.0** (it is wrapped in
-- MAGIC > `coalesce(..., 0.0)`). This is **expected until the bug is fixed** — the Data Quality section
-- MAGIC > below surfaces it explicitly so you can recognise the fingerprint rather than chase a phantom.
-- MAGIC
-- MAGIC **Glossary** (also mirrored to the UC table/column COMMENTs — see the DESCRIBE cell at the end):
-- MAGIC `coordinado` = scheduled / dispatched generation · `real` = actual measured · `reducciones` =
-- MAGIC curtailment · `conglomerate` = country-level aggregate · `hub`/`link`/`sat` = Data Vault (silver).
-- MAGIC
-- MAGIC ---
-- MAGIC #### How to run
-- MAGIC 1. Attach a SQL warehouse (top right).
-- MAGIC 2. Run top to bottom.
-- MAGIC
-- MAGIC #### Portability note
-- MAGIC All names are hardcoded to catalog `workspace` and schema prefix `dev_fuad_onate`. To run in a
-- MAGIC different sandbox, **find-replace `dev_fuad_onate` with your own short_name prefix** (and `workspace`
-- MAGIC with your catalog). ⚠️ Watch the one irregular name: the conglomerate **GOLD** schema is
-- MAGIC `..._conglomerate_gold_energy` — it has **NO `_chile` suffix**, unlike every other schema.
-- MAGIC
-- MAGIC #### Table of contents
-- MAGIC 1. Bronze — ingestion health & freshness
-- MAGIC 2. Silver — modeled (Data Vault) + coordinated-SCD health check
-- MAGIC 3. Gold — business metrics
-- MAGIC 4. Conglomerate — country-level domain
-- MAGIC 5. Data Quality — cross-layer row-count reconciliation & null rates
-- MAGIC 6. Pipeline runs — DLT event log
-- MAGIC 7. Data dictionary — UC comments (DESCRIBE)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Resource domain — BRONZE (ingestion & freshness)
-- MAGIC Raw CSV as ingested by Auto Loader (`;`-separated source; `valor` carries decimal commas).
-- MAGIC **What to look for:** rows present, `last_load` recent, `rescued` (malformed-CSV rows) ideally 0,
-- MAGIC and several distinct `file_path` / `resource` values landed.

-- COMMAND ----------

-- Bronze ingest health for the real solar feed.
-- WATCH: rescued > 0 means malformed CSV rows were captured in _rescued_data.
SELECT
  COUNT(*)                     AS rows,
  COUNT(_rescued_data)         AS rescued,
  COUNT(DISTINCT file_path)    AS files,
  COUNT(DISTINCT resource)     AS resources,
  MAX(modification_date)       AS last_load
FROM workspace.dev_fuad_onate_renewable_bronze_energy_chile.bronze_real_solar;

-- COMMAND ----------

-- Most recent raw rows (deterministic preview: ordered before LIMIT).
-- One of only two raw SELECT * previews kept in this notebook.
SELECT *
FROM workspace.dev_fuad_onate_renewable_bronze_energy_chile.bronze_real_solar
ORDER BY Fecha, Hora, Nombre
LIMIT 50;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Resource domain — SILVER (Data Vault: hub / link / sat)
-- MAGIC `silver_sat_measure` consolidates plant (real) vs reductions (curtailment) vs coordinated (dispatched)
-- MAGIC at one row per (real_name, record_date).
-- MAGIC **What to look for:** `plant_value` and `reduction_value` populated; `coordinated_value` is
-- MAGIC **mostly NULL** — that is the known PR #21 bug, quantified in the Data Quality section, not a query error.

-- COMMAND ----------

-- Deterministic, representative preview of the consolidated satellite (projected columns, ordered).
SELECT real_name, reduction_name, coordinated_name, record_date,
       plant_value, reduction_value, coordinated_value
FROM workspace.dev_fuad_onate_renewable_silver_energy_chile.silver_sat_measure
ORDER BY record_date, real_name
LIMIT 100;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Coordinated-SCD health check (PR #21 fingerprint)
-- MAGIC Compares the three silver hubs side by side. **Expected (buggy) result:** `coordinated` row count
-- MAGIC is far smaller than `plant` / `reductions` (e.g. ~7 vs ~42). When PR #21 is fixed the three counts
-- MAGIC should be roughly equal.

-- COMMAND ----------

-- Hub fan-out: coordinated << plant/reductions reveals the SCD-key collapse.
SELECT 'plant'       AS hub, COUNT(*) AS rows
  FROM workspace.dev_fuad_onate_renewable_silver_energy_chile.silver_hub_plant
UNION ALL
SELECT 'reductions',         COUNT(*)
  FROM workspace.dev_fuad_onate_renewable_silver_energy_chile.silver_hub_reductions
UNION ALL
SELECT 'coordinated',        COUNT(*)
  FROM workspace.dev_fuad_onate_renewable_silver_energy_chile.silver_hub_coordinated
ORDER BY hub;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Trace one entity bronze → silver
-- MAGIC Row counts per plant name across layers. The grain changes on purpose: bronze is **hourly**
-- MAGIC (`Fecha` + `Hora`, many rows per plant) while silver is **consolidated** (fewer rows). Counts are
-- MAGIC therefore NOT expected to match — this confirms a plant survives the modeling step, not equality.

-- COMMAND ----------

-- Same plant name (Nombre -> real_name) across two layers.
SELECT 'bronze_real_solar' AS layer, Nombre AS plant, COUNT(*) AS rows
  FROM workspace.dev_fuad_onate_renewable_bronze_energy_chile.bronze_real_solar
 GROUP BY Nombre
UNION ALL
SELECT 'silver_sat_measure', real_name, COUNT(*)
  FROM workspace.dev_fuad_onate_renewable_silver_energy_chile.silver_sat_measure
 WHERE real_name IS NOT NULL
 GROUP BY real_name
ORDER BY plant, layer;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. Resource domain — GOLD (business metrics)
-- MAGIC Curated aggregates. **What to look for:** `avg_real` / `avg_reductions` populated; `avg_coordinated`
-- MAGIC mostly **0.0** (coalesce-masked NULL from PR #21), which also drives `diff_real_coordinated`.

-- COMMAND ----------

-- GOLD daily averages (projected, ordered).
SELECT record_date, avg_real, avg_reductions, avg_coordinated, diff_real_coordinated
FROM workspace.dev_fuad_onate_renewable_gold_energy_chile.gold_daily_measure
ORDER BY record_date;

-- COMMAND ----------

-- Remaining curated gold tables (weekly / monthly grain + the real-vs-coordinated diff series).
SELECT * FROM workspace.dev_fuad_onate_renewable_gold_energy_chile.gold_weekly_measure  ORDER BY 1;

-- COMMAND ----------

SELECT * FROM workspace.dev_fuad_onate_renewable_gold_energy_chile.gold_monthly_measure ORDER BY 1;

-- COMMAND ----------

-- Tied to PR #21: coordinated series is collapsed, so this diff is largely uninformative until fixed.
SELECT * FROM workspace.dev_fuad_onate_renewable_gold_energy_chile.gold_different_real_coordinated ORDER BY 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. Conglomerate domain (country-level)
-- MAGIC Aggregated country comparisons (Chile vs LATAM / World) and increase metrics.
-- MAGIC ⚠️ Reminder: the GOLD schema here is `..._conglomerate_gold_energy` (**no `_chile`**).

-- COMMAND ----------

-- Conglomerate GOLD: Chile vs LATAM / World (note schema name has NO _chile suffix).
SELECT * FROM workspace.dev_fuad_onate_conglomerate_gold_energy.gold_different_renewable_again_chile ORDER BY year;

-- COMMAND ----------

SELECT * FROM workspace.dev_fuad_onate_conglomerate_gold_energy.gold_increase_production  ORDER BY 1;

-- COMMAND ----------

SELECT * FROM workspace.dev_fuad_onate_conglomerate_gold_energy.gold_increase_consumption ORDER BY 1;

-- COMMAND ----------

-- Conglomerate SILVER dimension (countries).
SELECT * FROM workspace.dev_fuad_onate_conglomerate_silver_energy_chile.silver_dim_country ORDER BY Entity;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5. Data Quality — cross-layer reconciliation & null rates
-- MAGIC The notebook's core question: **did data flow bronze → silver → gold?** These aggregates answer it
-- MAGIC at a glance (and a `LIMIT 100` preview never could).

-- COMMAND ----------

-- Per-layer row counts + freshness. Presence/propagation check (grains differ, so counts won't match).
-- NOTE: for silver/gold, the date column is the business record_date, not an ingestion timestamp.
SELECT 'bronze_real_solar'  AS tbl, COUNT(*) AS rows, MAX(modification_date) AS latest_date
  FROM workspace.dev_fuad_onate_renewable_bronze_energy_chile.bronze_real_solar
UNION ALL
SELECT 'silver_hub_plant',         COUNT(*), CAST(NULL AS TIMESTAMP)
  FROM workspace.dev_fuad_onate_renewable_silver_energy_chile.silver_hub_plant
UNION ALL
SELECT 'silver_hub_coordinated',   COUNT(*), CAST(NULL AS TIMESTAMP)
  FROM workspace.dev_fuad_onate_renewable_silver_energy_chile.silver_hub_coordinated
UNION ALL
SELECT 'silver_sat_measure',       COUNT(*), CAST(MAX(record_date) AS TIMESTAMP)
  FROM workspace.dev_fuad_onate_renewable_silver_energy_chile.silver_sat_measure
UNION ALL
SELECT 'gold_daily_measure',       COUNT(*), CAST(MAX(record_date) AS TIMESTAMP)
  FROM workspace.dev_fuad_onate_renewable_gold_energy_chile.gold_daily_measure
ORDER BY tbl;

-- COMMAND ----------

-- SILVER null-rate profile. coordinated near 0% populated == PR #21 fingerprint.
SELECT
  COUNT(*)                                                 AS rows,
  COUNT(plant_value)                                       AS plant_non_null,
  COUNT(reduction_value)                                   AS reduction_non_null,
  COUNT(coordinated_value)                                 AS coordinated_non_null,
  ROUND(100.0 * COUNT(coordinated_value) / COUNT(*), 1)    AS pct_coordinated_populated
FROM workspace.dev_fuad_onate_renewable_silver_energy_chile.silver_sat_measure;

-- COMMAND ----------

-- GOLD null/zero profile. avg_coordinated is coalesce(...,0.0)-masked, so count ZEROS, not NULLs.
SELECT
  COUNT(*)                                                       AS days,
  SUM(CASE WHEN avg_coordinated = 0 THEN 1 ELSE 0 END)          AS days_coordinated_zero,
  SUM(CASE WHEN avg_coordinated IS NULL THEN 1 ELSE 0 END)      AS days_coordinated_null,
  ROUND(100.0 * SUM(CASE WHEN avg_coordinated = 0 THEN 1 ELSE 0 END) / COUNT(*), 1)
                                                                AS pct_coordinated_zero
FROM workspace.dev_fuad_onate_renewable_gold_energy_chile.gold_daily_measure;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 6. Pipeline runs — DLT event log
-- MAGIC The authoritative "did the last run succeed?" source. The event-log table name embeds the pipeline
-- MAGIC id, so **discover the exact name first**, then paste it into the status query below.

-- COMMAND ----------

-- Discover the event-log table name (embeds the pipeline id).
SHOW TABLES IN workspace.dev_fuad_onate_renewable_bronze_energy_chile LIKE 'event_log*';

-- COMMAND ----------

-- Latest pipeline update states — pre-filled for the RESOURCE pipeline's event log.
-- (If you redeploy, the id changes: re-run the SHOW TABLES cell above and swap the name.)
-- Expect the most recent state to be COMPLETED; FAILED/CANCELED means you may be exploring stale data.
SELECT timestamp,
       details:update_progress.state AS state
FROM workspace.dev_fuad_onate_renewable_bronze_energy_chile.`event_log_9bcfc32a_19f0_4327_b5bc_e8ee1cc4a3a4`
WHERE event_type = 'update_progress'
ORDER BY timestamp DESC
LIMIT 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 7. Data dictionary — UC comments
-- MAGIC The data-dictionary COMMENTs were added to Unity Catalog; surface them so column meanings
-- MAGIC (coordinado / real / reducciones, hub/link/sat) are discoverable without leaving the notebook.

-- COMMAND ----------

-- Column-level comments for the consolidated satellite.
DESCRIBE TABLE EXTENDED workspace.dev_fuad_onate_renewable_silver_energy_chile.silver_sat_measure;

-- COMMAND ----------

-- All table comments at a glance (excludes DLT event_log_* internal tables).
SELECT table_schema, table_name, comment
FROM workspace.information_schema.tables
WHERE table_schema LIKE 'dev_fuad_onate_%'
  AND table_name NOT LIKE 'event_log_%'
  AND comment IS NOT NULL
ORDER BY table_schema, table_name;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Appendix — browse every sample table
-- MAGIC Enriched catalog listing (type / comment / last-altered). The `event_log_*` DLT tables are filtered
-- MAGIC out here; reach them via the discovery cell in section 6.

-- COMMAND ----------

-- Correct internal-table filter: the DLT tables are 'event_log_<id>', not '__'-prefixed.
SELECT table_schema, table_name, table_type, comment, last_altered
FROM workspace.information_schema.tables
WHERE table_schema LIKE 'dev_fuad_onate_%'
  AND table_name NOT LIKE 'event_log_%'
ORDER BY table_schema, table_name;