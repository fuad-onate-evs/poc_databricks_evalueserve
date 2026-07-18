# Renewable-Energy PoC — Technical Documentation

*Medallion lakehouse on Databricks | updated 2026-07-17*

## 1. Architecture

Databricks Asset Bundle with DLT (Delta Live Tables) pipelines on serverless, Auto Loader
(cloudFiles) ingestion from Unity Catalog Volumes, and Unity Catalog governance. Flow:
Sources → Ingestion → Bronze → Silver → Gold → Consumption, orchestrated by Databricks Jobs and
monitored with Lakehouse Data-Quality Monitors.

![Architecture](architecture.png)

![End-to-end workflow](workflow.png)

## 2. Domains & data sources

- **Weather** (`renewable_energy_chile` weather tables): solar irradiance + wind resource.
  Sources by role: **NASA POWER** = historical hourly (measured-equivalent), **DMC** = real-time
  measured stations, **MinEnergía/DGF** = modeled resource (typical year / 1980–2017).
- **Resource** (`renewable_energy_chile` generation): per-plant generation coordinado/real/reducciones.
  Source: **CEN / Coordinador**. Silver = Data Vault (hub/link/sat).
- **Conglomerate** (`renewable_conglomerate_energy`): country stats (Entity/Year). Source: **OWID**.

## 3. Data acquisition (fetchers → UC Volume → Auto Loader)

| Fetcher (`scripts/`) | Source | Endpoint / access | Notes |
|---|---|---|---|
| `nasa_power_fetcher.py` | NASA POWER | `power.larc.nasa.gov/api/temporal/hourly/point` (no auth) | historical hourly by lat/lon; fetched in yearly chunks; DMC-equivalent fields |
| `dgf_explorador_fetcher.py` | DGF Explorador | `python-router` (no auth) | modeled typical-year |
| `minenergia_api_fetcher.py` | MinEnergía official API | `POST /api/proxy` (session cookie) | hourly 1980–2017 |
| `cen_coordinador_fetcher.py` | CEN / Coordinador | `sipubv1.api.coordinador.cl/api/v1` (public key) | generation per plant; public by law |
| `owid_conglomerate_fetcher.py` | OWID | grapher CSVs (no auth) | 5 country datasets |
| *(DMC — real-time)* | DMC / meteochile | `getDatosRecientesEma/<code>?usuario&token` | measured, last 12 h, per-minute |

## 4. Weather — NASA POWER historical load (this cycle)

- `nasa_power_fetcher.py` pulls hourly `ALLSKY_SFC_SW_DWN` (GHI), `T2M`, `RH2M`, `PS`,
  `PRECTOTCORR`, `WS10M/WD10M/WS50M` per plant coordinate, in **yearly chunks** (the API caps the
  range per request).
- **Full load: 876,720 rows** (10 plants × 87,672 hours, **2015–2024**) → UC Volume →
  bronze `bronze_weather_nasa_power` (`read_files`).
- Physically validated: midday GHI highest for the Atacama plants (~871 W/m²).

## 5. Data quality, contracts & observability (Databricks-native)

- **Contracts:** documented per medallion step in [`../data-contracts.md`](../data-contracts.md).
- **In-pipeline DQ:** DLT `@dlt.expect_*` on the silver layers (Chile-bounds coords, known resource, has-metric, valid keys/dates) + UC constraints.
- **Lineage & glossary:** Unity Catalog automatic lineage + column comments; [`../glossary.md`](../glossary.md).
- **Monitoring:** three **Data-Quality Monitors** on the gold tables (profiling + drift, each with an auto-generated dashboard) + a **SQL alert** that fires on failed expectations.

## 6. Deploy & run

```bash
databricks bundle deploy -t dev --var=dev_catalog=workspace
databricks bundle run weather_streaming_job    -t dev --var=dev_catalog=workspace   # Weather
databricks bundle run conglomerate_owid_job     -t dev --var=dev_catalog=workspace   # Conglomerate (OWID)
databricks bundle run cen_generation_job        -t dev --var=dev_catalog=workspace   # Resource (CEN)
```

All work is done in **dev** and on branches. Prod deploys are managed by the project lead
(prod target scoped to their workspace). Note: deploying from a branch that lacks a resource
deletes it in dev — deploy from a branch that contains everything, or merge first.

## 7. Verify the medallion & jobs

- **Resource pipeline:** https://dbc-54b27bae-2e91.cloud.databricks.com/pipelines/9bcfc32a-19f0-4327-b5bc-e8ee1cc4a3a4?o=7474645896934260
- **Conglomerate pipeline:** https://dbc-54b27bae-2e91.cloud.databricks.com/pipelines/b5d63282-a439-42ef-8e3b-cd707a9d5f58?o=7474645896934260
- **Weather pipeline:** https://dbc-54b27bae-2e91.cloud.databricks.com/pipelines/c108b287-98cc-43d6-86d6-5b63a9e6b4ae?o=7474645896934260
- **Weather Job (fetch → medallion):** https://dbc-54b27bae-2e91.cloud.databricks.com/jobs/503330163541320?o=7474645896934260
- **Bronze (NASA POWER, 876k rows):** `workspace.dev_fuad_onate_renewable_bronze_energy_chile.bronze_weather_nasa_power`
- **Gold — Resource:** https://dbc-54b27bae-2e91.cloud.databricks.com/explore/data/workspace/dev_fuad_onate_renewable_gold_energy_chile/gold_daily_measure?o=7474645896934260
- **Gold — Weather KPI:** https://dbc-54b27bae-2e91.cloud.databricks.com/explore/data/workspace/dev_fuad_onate_renewable_gold_energy_chile/weather_gold_resource_kpi?o=7474645896934260
- **Gold — Conglomerate:** https://dbc-54b27bae-2e91.cloud.databricks.com/explore/data/workspace/dev_fuad_onate_conglomerate_gold_energy/gold_different_renewable_again_chile?o=7474645896934260
- **Quality monitors:** [Resource](https://dbc-54b27bae-2e91.cloud.databricks.com/sql/dashboardsv3/01f1809adb1a199384b80e96c469a206?o=7474645896934260) · [Weather](https://dbc-54b27bae-2e91.cloud.databricks.com/sql/dashboardsv3/01f1809ae54812c08231e73a5350f27c?o=7474645896934260) · [Conglomerate](https://dbc-54b27bae-2e91.cloud.databricks.com/sql/dashboardsv3/01f1809ae61519be89d4cf09780d51d6?o=7474645896934260)
