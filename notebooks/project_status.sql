-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 🌱 Renewable-Energy PoC — Project Status
-- MAGIC ### Chilean renewable-energy medallion on Databricks · status as of 2026-06-19
-- MAGIC
-- MAGIC A working **bronze → silver → gold** medallion for two domains, end-to-end on a
-- MAGIC Databricks Asset Bundle + DLT, populated with **synthetic** sample data while the
-- MAGIC real feed is being unblocked. This notebook walks the current state with live
-- MAGIC queries, and points to the dashboards and the end-to-end dataflow.
-- MAGIC
-- MAGIC > ⚠️ **All figures below are synthetic** placeholders — the real DGF feed is pending
-- MAGIC > approval (Trello card #56). Do not read magnitudes / diffs / country comparisons as real.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1 · Executive summary
-- MAGIC
-- MAGIC | Area | Status |
-- MAGIC |---|---|
-- MAGIC | **Medallion pipeline** (2 domains × bronze/silver/gold) | 🟢 built & running on DLT |
-- MAGIC | **Sample data** end-to-end (both domains, populated + OPTIMIZEd) | 🟢 done |
-- MAGIC | **Governance** — 100% column comments + schema tags | 🟢 done (`feat/column-comments`) |
-- MAGIC | **AI/BI dashboards** — Medallion Health + End-to-End | 🟢 published |
-- MAGIC | **End-to-end dataflow** — interactive diagram + docs | 🟢 done (`end_to_end_workflow`) |
-- MAGIC | **DGF poller** — auth + landing ready | 🟢 code ready (`feat/card-56-dgf-poller`) |
-- MAGIC | **Real DGF data** (api.minenergia.cl) | 🟠 blocked — registration pending admin approval |
-- MAGIC | **Coordinated SCD key** (PR #21) | 🔴 known bug — coordinated series collapses |
-- MAGIC
-- MAGIC **Two domains**
-- MAGIC - **resource** (`renewable_energy_chile`) — solar/eólica generation in three flavors
-- MAGIC   (`coordinado` / `real` / `reducciones`), silver = Data-Vault hub/link/sat keyed on
-- MAGIC   plant (hourly), gold = daily/weekly/monthly measures + real-vs-coordinated diff.
-- MAGIC - **conglomerate** (`renewable_conglomerate_energy`) — country-level (`Entity`/`Year`)
-- MAGIC   stats → dim_country + fact tables → gold YoY increase + Chile-vs-LATAM/World.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2 · End-to-end dataflow
-- MAGIC
-- MAGIC `Sources → Ingestion → Bronze → Silver → Gold → Consumption`
-- MAGIC
-- MAGIC - **Interactive, data-aware diagram:** open the **`end_to_end_workflow`** notebook
-- MAGIC   (same folder) — live counts, DQ health, click-through nodes, Kafka toggle.
-- MAGIC - **Static / GitHub:** `docs/end-to-end-data-workflow.md` (Mermaid) and
-- MAGIC   `docs/end-to-end-workflow.html` (interactive page).
-- MAGIC - **Where Kafka fits:** it is the *optional* streaming branch. For the agreed **hourly**
-- MAGIC   cadence the design drops the broker — the poller lands JSON in a UC Volume and Auto
-- MAGIC   Loader ingests it (`docs/card-56-bronze-landing-design.md`).
-- MAGIC
-- MAGIC ```
-- MAGIC  DGF api.minenergia.cl ─┐                         ┌─ BRONZE ─ Auto Loader (cloudFiles)
-- MAGIC   (MAIN, pending)       ├─▶ poller ─▶ UC Volume ─▶│   SILVER ─ Data Vault / dim·fact
-- MAGIC  CEN gen (synthetic) ───┤        └┄▶ Kafka (opt) ┄┘   GOLD ─── measures · increase
-- MAGIC  OWID country CSV ──────┘                              └─▶ dashboards · SQL · notebooks
-- MAGIC ```

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2.1 · Ingestion code — ready to activate
-- MAGIC The DGF feed has two landing options written in code (`src/renewable_energy_chile/streaming/`):
-- MAGIC - **Option A — recommended (hourly):** `bronze_dgf_autoloader.py` → poller lands JSON in a
-- MAGIC   UC Volume → Auto Loader DLT bronze (`bronze_dgf_met`). No broker.
-- MAGIC - **Option B — streaming (Kafka):** `bronze_dgf_kafka.py` + `scripts/dgf_kafka_producer.py`
-- MAGIC   → producer publishes to a topic → native `readStream.format("kafka")` bronze. Try it now
-- MAGIC   against the local broker: `python scripts/dgf_kafka_producer.py --demo 48`.
-- MAGIC
-- MAGIC Both are **inert until activated** (the pipeline glob only includes `transformations/**`);
-- MAGIC activation steps in `src/renewable_energy_chile/streaming/README.md`.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3 · The data — medallion at a glance
-- MAGIC Row counts of a representative table per layer × domain (live):

-- COMMAND ----------

SELECT 'bronze' AS layer, 'resource'     AS domain, 'bronze_real_solar'            AS sample_table, count(*) AS rows FROM workspace.dev_fuad_onate_renewable_bronze_energy_chile.bronze_real_solar
UNION ALL SELECT 'bronze','conglomerate','bronze_modern_renewable_prod',    count(*) FROM workspace.dev_fuad_onate_conglomerate_bronze_energy_chile.bronze_modern_renewable_prod
UNION ALL SELECT 'silver','resource',    'silver_sat_measure',             count(*) FROM workspace.dev_fuad_onate_renewable_silver_energy_chile.silver_sat_measure
UNION ALL SELECT 'silver','conglomerate','silver_fact_energy_production',   count(*) FROM workspace.dev_fuad_onate_conglomerate_silver_energy_chile.silver_fact_energy_production
UNION ALL SELECT 'gold',  'resource',    'gold_daily_measure',             count(*) FROM workspace.dev_fuad_onate_renewable_gold_energy_chile.gold_daily_measure
UNION ALL SELECT 'gold',  'conglomerate','gold_increase_production',        count(*) FROM workspace.dev_fuad_onate_conglomerate_gold_energy.gold_increase_production
ORDER BY layer, domain;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 3.1 · Bronze (raw) — Auto Loader landing
-- MAGIC Raw CSV as ingested: Spanish plant name, date/hour, comma-decimal `valor`, plus
-- MAGIC `_rescued_data` + `file_path` lineage. Every column is documented (hover the headers).

-- COMMAND ----------

SELECT Nombre, Fecha, Hora, valor, file_path, resource
FROM workspace.dev_fuad_onate_renewable_bronze_energy_chile.bronze_real_solar
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 3.2 · Silver (modeled) — Data-Vault satellite
-- MAGIC `silver_sat_measure` consolidates the three sources per plant·timestamp: `plant_value`
-- MAGIC (real), `reduction_value` (curtailment), `coordinated_value` (programmed).

-- COMMAND ----------

SELECT real_name, record_date, plant_value, reduction_value, coordinated_value
FROM workspace.dev_fuad_onate_renewable_silver_energy_chile.silver_sat_measure
WHERE real_name IS NOT NULL
ORDER BY record_date
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 3.3 · Gold (analytics) — daily measures + diff
-- MAGIC `gold_daily_measure` — daily averages and `diff_real_coordinated` (actual − programmed).
-- MAGIC Note `avg_coordinated` is ~0 → see the PR #21 data-quality check in §4.

-- COMMAND ----------

SELECT record_date, avg_real, avg_coordinated, avg_reductions, diff_real_coordinated
FROM workspace.dev_fuad_onate_renewable_gold_energy_chile.gold_daily_measure
ORDER BY record_date
LIMIT 12;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 3.4 · Gold (conglomerate) — Chile vs the world
-- MAGIC `gold_different_renewable_again_chile` — Chile's renewable share minus LATAM / World, by year.

-- COMMAND ----------

SELECT year, chile_vs_latam, chile_vs_world
FROM workspace.dev_fuad_onate_conglomerate_gold_energy.gold_different_renewable_again_chile
ORDER BY year;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4 · Data quality
-- MAGIC The one open data bug: the **coordinated** Data-Vault hub collapses (its SCD key uses
-- MAGIC only `hash_key`, so most plants drop out) → `avg_coordinated` ≈ 0 in gold. Fix is on
-- MAGIC branch **`fix/silver-scd-keys`** (PR #21).

-- COMMAND ----------

SELECT
  (SELECT count(*) FROM workspace.dev_fuad_onate_renewable_silver_energy_chile.silver_hub_plant)       AS plant_rows,
  (SELECT count(*) FROM workspace.dev_fuad_onate_renewable_silver_energy_chile.silver_hub_coordinated) AS coordinated_rows,
  CASE WHEN (SELECT count(*) FROM workspace.dev_fuad_onate_renewable_silver_energy_chile.silver_hub_coordinated)
          < (SELECT count(*) FROM workspace.dev_fuad_onate_renewable_silver_energy_chile.silver_hub_plant) * 0.5
       THEN '🔴 WARN — coordinated series collapsed (PR #21)'
       ELSE '🟢 OK' END AS pr21_check;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 4.1 · Governance — field-level documentation
-- MAGIC 100% of user-facing columns carry comments (set in pipeline-code schemas). Sample:

-- COMMAND ----------

SELECT column_name, comment
FROM workspace.information_schema.columns
WHERE table_schema = 'dev_fuad_onate_renewable_bronze_energy_chile'
  AND table_name   = 'bronze_coordinated_solar'
ORDER BY ordinal_position;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5 · Dashboards (AI/BI)
-- MAGIC
-- MAGIC - 📊 **Medallion Health** — counts + health checks, flags the PR #21 bug
-- MAGIC   → [open](https://dbc-54b27bae-2e91.cloud.databricks.com/dashboardsv3/01f16ab0ca651edcbdf72c72216bd590/published)
-- MAGIC - 🔄 **Medallion End-to-End** — ingestion → counts → gold metrics → pipeline runs → DQ (Santiago-time)
-- MAGIC   → [open](https://dbc-54b27bae-2e91.cloud.databricks.com/dashboardsv3/01f16b2f58c216feb8dcb519d29b8adb/published)
-- MAGIC - 🧭 **End-to-end dataflow** (interactive notebook) → `end_to_end_workflow` (same folder)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 6 · What's next
-- MAGIC
-- MAGIC 1. **Unblock real data (card #56)** — chase the `api.minenergia.cl` approval
-- MAGIC    (`ernc@dgf.uchile.cl`). The **`dgf_poller.py`** is ready: on approval, set
-- MAGIC    `DGF_ENDPOINTS`, run it (locally or as an hourly Job) → files land → bronze ingests.
-- MAGIC    No structural change — synthetic flips to real.
-- MAGIC 2. **Merge `fix/silver-scd-keys` (PR #21)** — restores the coordinated series.
-- MAGIC 3. **Merge the open PRs** — column comments, P0 fixes, perf, docs.
-- MAGIC
-- MAGIC ### Appendix — where things live
-- MAGIC | What | Where |
-- MAGIC |---|---|
-- MAGIC | Pipelines | `src/renewable_energy_chile/`, `src/renewable_conglomerate_energy/` |
-- MAGIC | Poller | `scripts/dgf_poller.py` (+ `.env.example`) |
-- MAGIC | Dataflow diagram | `notebooks/end_to_end_workflow`, `docs/end-to-end-*` |
-- MAGIC | Governance / catalog | `notebooks/data_catalog`, `docs/glossary.md`, `docs/data-governance.md` |
-- MAGIC | Walkthrough | `notebooks/pipeline_walkthrough`, `notebooks/sample_data_queries` |
