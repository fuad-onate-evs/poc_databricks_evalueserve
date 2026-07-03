# Renewable-Energy PoC — Executive Summary

*Chilean renewable-energy data platform on Databricks | 2026-07-02*

> 📄 Also available as [`executive-summary.pdf`](executive-summary.pdf).

## Overview

A proof of concept for a renewable-energy data platform on Databricks using a medallion (bronze → silver → gold) lakehouse. It ingests real data from the data owner and turns it into analytics-ready metrics, fully automated and governed.

## Delivered — Weather streaming solution

An end-to-end pipeline from the DGF (U. de Chile, Geophysics Dept.) API to the gold layer:

- API (DGF Explorador, no login) → Databricks Volume → Bronze (Auto Loader, streaming) → Silver (clean + data quality) → Gold (per-plant resource KPI).
- Orchestrated by an hourly Databricks Job. Real data for 10 Chilean solar/wind plants.

## Results (verified in dev)

- The job runs end-to-end (fetch + medallion) on real data.
- Gold: 5 solar + 5 wind plants, ranked by resource with an A/B/C quality tier.
- Example: Cerro Dominador GHI 7.28 (A); Negrete Cuel wind 7.85 m/s (A).

## Governance & quality (Databricks-native)

- Data quality: DLT expectations. Lineage: Unity Catalog (automatic). Glossary: UC tags + column comments.
- No external tools required (OpenMetadata / Great Expectations not needed).

## Status & next steps

- Delivered and verified in dev. Pending: review + merge (PR #24) → deploy to prod (activates the hourly schedule).
- The gated official API (api.minenergia.cl) is a drop-in source swap once access is approved; the open DGF Explorador already provides real data.

## Explore / test the results

- **Job (fetch → medallion):** https://dbc-54b27bae-2e91.cloud.databricks.com/jobs/503330163541320?o=7474645896934260
- **Pipeline DLT (bronze→silver→gold):** https://dbc-54b27bae-2e91.cloud.databricks.com/pipelines/c108b287-98cc-43d6-86d6-5b63a9e6b4ae?o=7474645896934260
- **Gold table (result):** https://dbc-54b27bae-2e91.cloud.databricks.com/explore/data/workspace/dev_fuad_onate_renewable_gold_energy_chile/weather_gold_resource_kpi?o=7474645896934260
- **Code (PR #24):** https://github.com/oxiboy/poc_databricks_evalueserve/pull/24
