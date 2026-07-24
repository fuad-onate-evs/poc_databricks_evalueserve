# Renewable-Energy PoC — Executive Summary

*Chilean renewable-energy data platform on Databricks | updated 2026-07-17*

## Overview

A proof of concept for a renewable-energy data platform on Databricks using a **medallion**
(bronze → silver → gold) lakehouse. It ingests **real data from four independent sources**,
turns it into analytics-ready metrics across three domains, and is fully governed and monitored —
all in a **dev** environment, on isolated branches; production is the project lead's.

## Four data sources connected — all on real data

| Domain | Source | Access | Cadence |
|---|---|---|---|
| **Weather** | NASA POWER · DMC · MinEnergía/DGF | open / token / session | historical hourly + real-time |
| **Resource** (generation) | CEN / Coordinador | **public by law** (Art. 72-8, Ley 20.936) — no cost | daily per plant |
| **Conglomerate** (country) | Our World in Data (OWID) | open CSV | annual |

**Key unblocks this cycle:**
- **CEN generation** was assumed to need a paid/registered SIPUB account. It is **public information by law** and reachable with the public key the Coordinador's own site exposes — no registration, no cost.
- **Historical weather** with the same fields as the DMC station network is served cleanly by **NASA POWER**: a **10-year hourly full load (876,720 rows)** is loaded in dev.

## Results (verified in dev)

- **Medallion runs end-to-end for all three domains** (bronze → silver → gold).
- **Weather**: 10-year historical hourly loaded (physically validated — Atacama shows the highest irradiance), modeled as a **star schema + marts** (daily/monthly/KPI), with an **AI/BI dashboard** (time series + KPIs).
- **Resource**: real per-plant generation (coordinado / real / reducciones).
- **Conglomerate**: Chile-vs-World renewables lead grew +19 → +38 (2017–2024).

## Governance & data quality (Databricks-native)

- **Data contracts** documented for every medallion step; **DLT expectations** enforce quality in-pipeline.
- **Data glossary** (business + technical) and Unity Catalog lineage + column comments.
- **Observability**: three Lakehouse **Data-Quality Monitors** on the gold tables (profiling + drift, auto-generated dashboards) + a **SQL alert** on failed expectations.

## Status & next steps

- Delivered and verified in dev, across independent branches / PRs (#24–#28). Pending: review + merge → deploy to prod (project lead).
- A cadence reality was documented: most public sources are **not** real-time (DGF/MinEnergía are historical; CEN lags months). The **DMC** feed is the only real-time measured source; **NASA POWER** covers history.

## Explore / test the live results

- **Resource pipeline:** https://dbc-54b27bae-2e91.cloud.databricks.com/pipelines/9bcfc32a-19f0-4327-b5bc-e8ee1cc4a3a4?o=7474645896934260
- **Conglomerate pipeline:** https://dbc-54b27bae-2e91.cloud.databricks.com/pipelines/b5d63282-a439-42ef-8e3b-cd707a9d5f58?o=7474645896934260
- **Weather pipeline:** https://dbc-54b27bae-2e91.cloud.databricks.com/pipelines/c108b287-98cc-43d6-86d6-5b63a9e6b4ae?o=7474645896934260
- **Data-quality monitor (Resource gold):** https://dbc-54b27bae-2e91.cloud.databricks.com/sql/dashboardsv3/01f1809adb1a199384b80e96c469a206?o=7474645896934260
- **Pull requests:** https://github.com/oxiboy/poc_databricks_evalueserve/pulls
