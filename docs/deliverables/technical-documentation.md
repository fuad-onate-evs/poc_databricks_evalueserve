# Renewable-Energy PoC — Technical Documentation

*Medallion lakehouse on Databricks | 2026-07-02*

> 📘 Also available as [`technical-documentation.pdf`](technical-documentation.pdf).

## 1. Architecture

Databricks Asset Bundle with DLT (Delta Live Tables) pipelines on serverless, Auto Loader (cloudFiles) streaming ingestion from Unity Catalog Volumes, and Unity Catalog governance. Flow: Sources → Ingestion → Bronze → Silver → Gold → Consumption, orchestrated by hourly Jobs.

![Architecture — Renewable-Energy PoC](architecture.png)

## 2. Domains & data sources

- **Resource** (`renewable_energy_chile`): generation per plant (coordinado/real/reducciones). Source: CEN / Coordinador Eléctrico Nacional.
- **Conglomerate** (`renewable_conglomerate_energy`): country stats (Entity/Year). Source: Our World in Data (OWID).
- **Weather** (delivered): solar irradiance + wind resource. Source: DGF Explorador (solar/eolico.minenergia.cl). The DGF owns the resource/weather data, not the generation.

## 3. Weather pipeline (this deliverable)

- **Acquisition:** `scripts/dgf_explorador_fetcher.py` calls the open python-router endpoint (no auth), lands JSON in the UC Volume (`lookup/dgf/`). Retries transient empty responses.
- **Bronze:** `weather_bronze` — Auto Loader streaming table (cloudFiles json) over the Volume; raw + file lineage.
- **Silver:** `weather_silver` — flatten/type per plant; keep latest fetch per plant; DLT expectations (known resource, Chile-bounds coords, has a metric).
- **Gold:** `weather_gold_resource_kpi` — per-plant KPI: headline metric (solar GHI / wind m/s), rank within resource, A/B/C tier.
- **Orchestration:** `weather_streaming_job` (hourly) = `fetch_weather` (serverless python) → `weather_medallion` (DLT pipeline).

## 4. Data quality, lineage & glossary (Databricks-native)

- **Data quality / contracts:** DLT `@dlt.expect_all_or_drop` (observable in the DLT event log) + Unity Catalog constraints.
- **Lineage:** Unity Catalog automatic table + column lineage (Catalog Explorer → Lineage).
- **Glossary:** UC tags + column comments (100% of columns documented) + `docs/glossary.md`.

## 5. Deploy & run

```bash
databricks bundle deploy -t dev --var=dev_catalog=workspace
databricks bundle run weather_streaming_job -t dev --var=dev_catalog=workspace
```

Prod deploys are managed by the project lead (prod target scoped to their workspace).

## Explore / test the results

- **Job (fetch → medallion):** https://dbc-54b27bae-2e91.cloud.databricks.com/jobs/503330163541320?o=7474645896934260
- **Pipeline DLT (bronze→silver→gold):** https://dbc-54b27bae-2e91.cloud.databricks.com/pipelines/c108b287-98cc-43d6-86d6-5b63a9e6b4ae?o=7474645896934260
- **Gold table (result):** https://dbc-54b27bae-2e91.cloud.databricks.com/explore/data/workspace/dev_fuad_onate_renewable_gold_energy_chile/weather_gold_resource_kpi?o=7474645896934260
- **Code (PR #24):** https://github.com/oxiboy/poc_databricks_evalueserve/pull/24
