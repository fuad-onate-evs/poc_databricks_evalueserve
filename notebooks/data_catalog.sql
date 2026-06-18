-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 🗂️ Data catalog · governance · glossary — `dev_fuad_onate` sandbox
-- MAGIC Auto-generated survey of the Unity Catalog `workspace` sandbox for this renewable-energy PoC:
-- MAGIC **what data exists**, **what it means** (comments + glossary), **how it is governed** (tags + grants), and **lineage**.
-- MAGIC
-- MAGIC > ⚠️ Data is **synthetic sample** (real feed pending). Every schema is tagged `data_classification = synthetic-sample`.
-- MAGIC
-- MAGIC **Sections:** 1) Schemas · 2) Tables · 3) Column dictionary · 4) Tags · 5) Grants · 6) Lineage · 7) Glossary · 8) Governance model
-- MAGIC
-- MAGIC *Portability: hardcoded to catalog `workspace` / prefix `dev_fuad_onate` — find-replace for another sandbox.*

-- COMMAND ----------
-- MAGIC %md ## 1. Schemas — medallion layers × domains (with descriptions)

-- COMMAND ----------
-- Schemas in the sandbox + their description (comment)
SELECT schema_name, comment
FROM workspace.information_schema.schemata
WHERE schema_name LIKE 'dev_fuad_onate_%'
ORDER BY schema_name;

-- COMMAND ----------
-- MAGIC %md ## 2. Tables inventory — type, description, last modified

-- COMMAND ----------
SELECT table_schema, table_name, table_type, comment, last_altered
FROM workspace.information_schema.tables
WHERE table_schema LIKE 'dev_fuad_onate_%'
  AND table_name NOT LIKE 'event_log_%'   -- exclude DLT internal event-log tables
ORDER BY table_schema, table_name;

-- COMMAND ----------
-- MAGIC %md ## 3. Column dictionary — every column, its type, and its UC comment
-- MAGIC The `comment` is the in-data **definition** (data dictionary). Search/filter this result.

-- COMMAND ----------
SELECT table_schema, table_name, column_name, data_type, comment
FROM workspace.information_schema.columns
WHERE table_schema LIKE 'dev_fuad_onate_%'
  AND table_name NOT LIKE 'event_log_%'
ORDER BY table_schema, table_name, ordinal_position;

-- COMMAND ----------
-- MAGIC %md ## 4. Governance — tags
-- MAGIC `domain` / `layer` / `medallion` / `data_classification` applied to every schema. Tags drive
-- MAGIC discovery, lineage filtering and policy (e.g. mask everything tagged `pii`).

-- COMMAND ----------
SELECT schema_name, tag_name, tag_value
FROM workspace.information_schema.schema_tags
WHERE schema_name LIKE 'dev_fuad_onate_%'
ORDER BY schema_name, tag_name;

-- COMMAND ----------
-- MAGIC %md ## 5. Governance — access grants
-- MAGIC Who can do what on a schema. Repeat for any object: `SHOW GRANTS ON TABLE ...`.

-- COMMAND ----------
SHOW GRANTS ON SCHEMA workspace.dev_fuad_onate_renewable_gold_energy_chile;

-- COMMAND ----------
-- MAGIC %md ## 6. Lineage
-- MAGIC Unity Catalog tracks **table & column lineage automatically** (bronze → silver → gold).
-- MAGIC `system.access.table_lineage` isn't query-able with this user's privileges on Free Edition, so:
-- MAGIC open any table in **Catalog Explorer → Lineage** to see upstream/downstream visually.

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## 7. Glossary — domain terms
-- MAGIC | Term | Meaning |
-- MAGIC |---|---|
-- MAGIC | **Coordinado / `coor`** | programmed / dispatched generation (the plan) |
-- MAGIC | **Real** | actual measured generation |
-- MAGIC | **Reducciones** | curtailment — energy cut / not injected; *preliminary* = provisional |
-- MAGIC | **diff_real_coordinated** | `real − coordinated` (deviation from the plan) |
-- MAGIC | **Resource domain** | plant-level Chilean solar & wind generation |
-- MAGIC | **Conglomerate domain** | country-level aggregated stats (`Entity` × `Year`) |
-- MAGIC | **Bronze / Silver / Gold** | medallion: raw → modeled → business metrics |
-- MAGIC | **Hub / Link / Satellite** | Data Vault modeling (silver) |
-- MAGIC | **CEN / Coordinador** | Chile grid operator (source of real/coordinado/reducciones) |
-- MAGIC | **DGF** | U. de Chile Dept. of Geophysics (met data, Trello card #56) |
-- MAGIC
-- MAGIC Full glossary: repo `docs/glossary.md` · notebook `/Users/fuad.onate@evalueserve.com/glossary`.

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## 8. Governance model — how this catalog is governed
-- MAGIC - **Structure:** catalog (`dev`/`prod`; here the writable `workspace` sandbox) → schemas per **domain × layer** (`<domain>_<layer>_energy_chile`) → tables.
-- MAGIC - **Naming:** medallion-prefixed tables (`bronze_*`, `silver_*`, `gold_*`); dev schemas prefixed `dev_<user>_`. ⚠️ one irregular name: conglomerate **gold** schema has **no `_chile` suffix**.
-- MAGIC - **Tags (§4):** `domain`, `layer`, `medallion`, `data_classification` on every schema → discovery & policy.
-- MAGIC - **Data dictionary (§2–3):** UC `COMMENT`s on tables/columns — definitions live with the data.
-- MAGIC - **Access (§5):** UC grants. **Prod should use a service-principal `run_as` + group-based grants** (not individuals — see repo `docs/repo-improvements.md` §1.5).
-- MAGIC - **Lineage (§6):** automatic in UC.
-- MAGIC - **Quality:** DLT **expectations** + the Medallion-Health & End-to-End **dashboards**.
-- MAGIC - **Sensitive data:** none here (synthetic). For real data: add **column masks / row filters** and a `pii` tag.
-- MAGIC
-- MAGIC Full model: repo `docs/data-governance.md`.

-- COMMAND ----------
-- MAGIC %md ## Appendix — full table inventory with comments (one-glance dictionary)

-- COMMAND ----------
SELECT table_schema, table_name, comment
FROM workspace.information_schema.tables
WHERE table_schema LIKE 'dev_fuad_onate_%'
  AND table_name NOT LIKE 'event_log_%'
  AND comment IS NOT NULL
ORDER BY table_schema, table_name;
