# poc_databricks_evalueserve

Databricks data platform for **Chilean renewable-energy** analytics — built as a **Databricks Asset Bundle** with **DLT / Lakeflow Declarative Pipelines** following the **medallion** architecture (bronze → silver → gold) on Unity Catalog.

## Domains

Two independent domains, each with its own DLT pipeline:

| Domain | Pipeline | What it models |
|---|---|---|
| **Resource** — `renewable_energy_chile` | `renewable_energy_chile` | Plant-level **solar & eólica (wind)** generation in three flavors: `coordinado` (programmed), `real` (actual), `reducciones` (curtailment). Silver is a Data-Vault hub/link/sat keyed on plant at **hourly** grain; gold computes monthly/daily/weekly averages and real-vs-coordinated differences. |
| **Conglomerate** — `renewable_conglomerate_energy` | `renewable_conglomerate` | Country-level (`Entity`/`Year`) renewable statistics (hydropower, installed solar PV, renewable production/consumption, electricity share). Silver builds `dim_country` + fact tables; gold computes year-over-year increases and Chile-vs-LATAM/World comparisons. |

## Architecture

```
CSV in Unity Catalog Volumes  ──Auto Loader──▶  BRONZE  ──▶  SILVER  ──▶  GOLD
 /Volumes/{catalog}/{schema}/lookup/...      (DLT streaming tables)   (materialized views)
```

- **Ingestion:** `bronze_*` tables use **Auto Loader** (`cloudFiles`, CSV) reading from Unity Catalog **Volumes** (`lookup/…`), with schema evolution and `_metadata` file lineage.
- **Compute:** serverless DLT pipelines, orchestrated by the **`full_execution`** job on a **daily** schedule (conglomerate → resource).
- **Environments:** `dev` (default; resources prefixed `[dev <user>]`, per-user schemas) and `prod`. Catalog/schema names come from `resources/variables.yml`.

## Repository layout

```
src/
  renewable_energy_chile/transformations/          # bronze / silver / gold + expectations (resource domain)
  renewable_conglomerate_energy/transformations/   # bronze / silver / gold (conglomerate domain)
  helper/                                          # shared PySpark column helpers (hashing, string norm, row-diff)
  run_unit_tests.py                                # pytest entry point (`uv run main`)
resources/
  variables.yml  schema.yml  volume.yml            # UC catalog/schema/volume + dev/prod variables
  pipeline/*.yml                                   # DLT pipeline definitions
  jobs/*.yml                                       # job orchestration
tests/                                             # pytest unit tests (use databricks-connect)
fixtures/                                          # test data fixtures
docs/                                              # design & discovery notes (see below)
databricks.yml                                     # bundle definition (targets, artifacts, variables)
```

## Prerequisites

- **Databricks CLI** — https://docs.databricks.com/dev-tools/cli/
- **uv** (Python package/venv manager) — https://docs.astral.sh/uv/
- **Python 3.10–3.12** — the project caps at `<3.13`; `uv` provisions a managed interpreter if needed.

## Getting started

```bash
# 1. Install dependencies into a local venv (Python 3.10–3.12)
uv sync --dev

# 2. Authenticate to the Databricks workspace
databricks auth login --host https://dbc-54b27bae-2e91.cloud.databricks.com

# 3. Validate the bundle (dev is the default target)
databricks bundle validate

# 4. Deploy a development copy (resources prefixed [dev <your-name>])
databricks bundle deploy -t dev

# 5. Run a pipeline or the full job
databricks bundle run renewable_energy_chile      # or: full_execution

# 6. Run unit tests (databricks-connect → serverless compute)
uv run pytest                                     # or: uv run main
```

Deploy to production with `databricks bundle deploy -t prod`.

## CI/CD

`.github/workflows/databricks_bundle_ci.yml` runs on `workflow_dispatch` (→ **dev**) and on an **approved** `pull_request_review` targeting `main` (→ **prod**). It installs the Databricks CLI + uv, logs in, **runs the unit tests, then validates and deploys** the bundle. Required repo secrets: `DATABRICKS_HOST`, `DATABRICK_ACCESS_TOKEN`.

## Documentation

Design and discovery notes live in [`docs/`](docs/):
- [`card-56-dgf-data-acquisition-discovery.md`](docs/card-56-dgf-data-acquisition-discovery.md) — how to acquire U. de Chile (DGF) meteorological data (hourly; primary source `api.minenergia.cl`).
- [`card-56-bronze-landing-design.md`](docs/card-56-bronze-landing-design.md) — landing a Kafka stream on bronze within the current architecture.
- [`repo-improvements.md`](docs/repo-improvements.md) — prioritized repo/bundle improvement backlog.
- [`databricks-features-medallion.md`](docs/databricks-features-medallion.md) — cost-aware Databricks feature-adoption checklist for the medallion.

## Observability & exploration

- **Pipeline monitoring** — the **DLT / Lakeflow pipeline UI** (Jobs & Pipelines → your pipeline) is the built-in observability app: flow graph, throughput, data-quality **expectations**, and the **event log**.
- **Medallion-health dashboard** — an **AI/BI dashboard** can track row counts, freshness and DQ per layer across both domains (built via the `lakeview` API; auto-flags issues such as the coordinated-SCD defect). See [`docs/databricks-features-medallion.md`](docs/databricks-features-medallion.md).
- **Ad-hoc exploration** — [`scripts/explore_sample_data.py`](scripts/explore_sample_data.py) queries the medallion tables via **Databricks Connect** (serverless, no warehouse) and prints rows; or use the **Databricks VS Code extension** Catalog Explorer / the SQL Editor.
- **Sample data** — exercise the whole medallion without the real feed by dropping `;`-CSVs (resource) / `,`-CSVs (conglomerate) into the bronze `lookup` Volume and running the pipeline.

## Development notes

- Dependencies are locked in **`uv.lock`** (committed) — use `uv sync --frozen` for reproducible installs.
- Lint: `uv run ruff check src tests` · Format: `uv run ruff format`.
- The two domains currently use different pipeline APIs (`import dlt` vs `from pyspark import pipelines as dp`); standardization is tracked in `docs/repo-improvements.md`.
