# Renewable-Energy PoC — Executive Summary

*Chilean renewable-energy data platform on Databricks | updated 2026-07-28*

## Overview

A proof of concept for a renewable-energy data platform on Databricks using a **medallion**
(bronze → silver → gold) lakehouse. It ingests **real data from independent public sources**,
turns it into analytics-ready metrics across **three domains** (Weather · Resource · Conglomerate),
and is fully governed, monitored and dashboarded — all in a **dev** environment, on isolated
branches; production is the project lead's.

## Data sources connected — all on real data

| Domain | Source | Access | Cadence |
|---|---|---|---|
| **Weather** | NASA POWER · DMC · MinEnergía/DGF | open / token / session | historical hourly + real-time + world daily |
| **Resource** (generation) | CEN / Coordinador | **public by law** (Art. 72-8, Ley 20.936) — no cost | daily per plant |
| **Conglomerate** (country) | Our World in Data (OWID) | open CSV | annual |

**Key unblocks:**
- **CEN generation** was assumed to need a paid/registered SIPUB account. It is **public information by law** and reachable with the public key the Coordinador's own site exposes — no registration, no cost.
- **Historical weather** with the same fields as the DMC station network is served cleanly by **NASA POWER** (10-year hourly full load, **876,720 rows**), and extended to a **12-city world benchmark** (all continents) for Chile-vs-world comparison.

## Weather officialized in a dedicated schema

All weather is consolidated in three purpose-built schemas — **`weather_bronze` / `weather_silver` / `weather_gold`** (dev catalog). Each layer holds **per-source tables** (`nasa_power`, `dmc`, `dgf`) **plus one consolidated `all_sources` table** carrying a `source` column, so every weather record — historical, real-time or modeled — lives under one governed roof (bronze 882k · silver 881k rows).

## Results (verified in dev)

- **Medallion runs end-to-end for all three domains** (bronze → silver → gold), cleaned and self-documented (schema + table comments, symmetric naming).
- **Weather**: 10-year historical hourly loaded (physically validated — Atacama shows the highest irradiance), modeled as a **star schema + marts** (daily/monthly/KPI), consolidated across sources, and served through **three AI/BI dashboards**:
  - **Chile & Rest of World** (one dashboard, two tabs) — seasonality, trends, max/min/means for temperature, GHI, pressure and wind; Rest-of-World tab shows the Northern-vs-Southern hemisphere inversion across 12 cities.
  - **Chile & Rest of World — 2025–2026 YTD** — the same views on the latest data.
  - **Weather NASA** — time series + resource KPIs.
- **Resource**: real per-plant generation (coordinado / real / reducciones).
- **Conglomerate**: Chile-vs-World renewables lead grew +19 → +38 (2017–2024).

## Governance & data quality (Databricks-native)

- **Data contracts** documented for every medallion step; **DLT expectations** enforce quality in-pipeline.
- **Data glossary** (business + technical) and Unity Catalog lineage + column/table comments.
- **Observability**: three Lakehouse **Data-Quality Monitors** on the gold tables (profiling + drift, auto-generated dashboards) + a **SQL alert** on failed expectations.

## Status & next steps

- Delivered and verified in dev, consolidated in delivery ticket **Trello #74**. The five PRs (#24–#28) were closed without merge as the team's `main` advanced on a separate track; the work remains on the fork branches, ready to be re-integrated (CI fix is the priority — the old CI deploys to prod on approval and runs no tests).
- A cadence reality was documented: most public sources are **not** real-time (DGF/MinEnergía are historical; CEN lags months). The **DMC** feed is the only real-time measured source; **NASA POWER** covers history and the world benchmark.

## Explore / test the live results

- **Weather — Chile & Rest of World dashboard (2 tabs):** https://dbc-54b27bae-2e91.cloud.databricks.com/sql/dashboardsv3/01f18a95d5851bb1a33415a71141a908?o=7474645896934260
- **Weather — Chile & Rest of World, 2025–2026 YTD:** https://dbc-54b27bae-2e91.cloud.databricks.com/sql/dashboardsv3/01f18a9c8d2f19a48485521da3f5a4f6?o=7474645896934260
- **Weather NASA dashboard (series + KPIs):** https://dbc-54b27bae-2e91.cloud.databricks.com/sql/dashboardsv3/01f18771593d19568e23e322277007c3?o=7474645896934260
- **Data-quality monitor (Resource gold):** https://dbc-54b27bae-2e91.cloud.databricks.com/sql/dashboardsv3/01f1809adb1a199384b80e96c469a206?o=7474645896934260
- **Pull requests:** https://github.com/oxiboy/poc_databricks_evalueserve/pulls
