-- Databricks notebook source
-- MAGIC %md
-- MAGIC # DGF Meteo Medallion — REAL resource data (bronze → silver → gold)
-- MAGIC
-- MAGIC Models the **real** DGF Explorador resource data (solar irradiance + wind, no-auth)
-- MAGIC for Chilean renewable plants as a medallion. Source chain:
-- MAGIC `scripts/dgf_explorador_fetcher.py` → UC Volume (`lookup/dgf/`) → here.
-- MAGIC
-- MAGIC This is the **meteorological** layer (resource potential), separate from the
-- MAGIC generation medallion. Verified end-to-end on 2026-06-24 with 10 real plants.
-- MAGIC Run top-to-bottom (needs a live SQL warehouse).

-- COMMAND ----------

-- MAGIC %md ## Bronze — raw JSON landed by the fetcher

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.default.bronze_dgf_explorador AS
SELECT * FROM read_files(
  '/Volumes/workspace/dev_fuad_onate_renewable_bronze_energy_chile/lookup/dgf/',
  format => 'json'
);
SELECT count(*) AS bronze_rows, count_if(resource='solar') AS solar, count_if(resource='eolic') AS wind
FROM workspace.default.bronze_dgf_explorador;

-- COMMAND ----------

-- MAGIC %md ## Silver — flatten + type, one row per plant

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.default.silver_dgf_resource AS
SELECT
  name              AS plant,
  resource,
  lat, lon,
  data.VR.GHI       AS ghi,      -- global horizontal irradiance (kWh/m2/day) — solar
  data.VR.DNI       AS dni,      -- direct normal irradiance (kWh/m2/day) — solar
  data.VR.TMP       AS temp_c,   -- mean temperature (C)
  data.VR.VEL       AS wind_ms,  -- mean wind speed (m/s) — present for both
  to_timestamp(fetched_at_utc) AS loaded_at
FROM workspace.default.bronze_dgf_explorador;

SELECT * FROM workspace.default.silver_dgf_resource ORDER BY resource, plant;

-- COMMAND ----------

-- MAGIC %md ## Gold — resource KPI per plant (headline metric + rank + quality tier)

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.default.gold_dgf_resource_kpi AS
SELECT
  plant, resource, lat, lon,
  round(CASE WHEN resource = 'solar' THEN ghi ELSE wind_ms END, 2) AS resource_metric,
  CASE WHEN resource = 'solar' THEN 'GHI kWh/m2/day' ELSE 'wind m/s' END AS unit,
  round(temp_c, 1) AS temp_c,
  rank() OVER (
    PARTITION BY resource
    ORDER BY (CASE WHEN resource = 'solar' THEN ghi ELSE wind_ms END) DESC
  ) AS rank_in_resource,
  CASE
    WHEN resource = 'solar' THEN
      CASE WHEN ghi >= 6.5 THEN 'A — excellent' WHEN ghi >= 5.5 THEN 'B — good' ELSE 'C — moderate' END
    ELSE
      CASE WHEN wind_ms >= 7 THEN 'A — excellent' WHEN wind_ms >= 5 THEN 'B — good' ELSE 'C — moderate' END
  END AS resource_tier
FROM workspace.default.silver_dgf_resource;

-- COMMAND ----------

-- MAGIC %md ## Result — the real-data medallion output

-- COMMAND ----------

SELECT resource, rank_in_resource AS rnk, plant, resource_metric, unit, temp_c, resource_tier
FROM workspace.default.gold_dgf_resource_kpi
ORDER BY resource, rank_in_resource;
