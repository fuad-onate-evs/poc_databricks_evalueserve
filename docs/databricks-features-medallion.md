# Maximizing Databricks features across the medallion — cost-aware

Goal: lean on **Databricks / Unity Catalog / Lakeflow** features as much as possible for this
renewable-energy lakehouse (instead of hand-rolled logic), while **favouring free / no-extra-cost
options**. A feature-adoption checklist mapped to bronze → silver → gold.

> **Cost legend** — nothing on Databricks is truly $0 (all compute bills DBUs), so:
> 💚 = **included**, no premium beyond the compute you already run (prefer these).
> 💲 = **adds recurring cost** (extra jobs/optimize compute/always-on/3rd-party) — adopt deliberately.
> The biggest cost lever is **triggered / serverless / `AvailableNow`** instead of always-on.

## Already adopted ✅
| Feature | Where | Cost |
|---|---|---|
| Unity Catalog (catalogs/schemas/**Volumes**) | `resources/{schema,volume,variables}.yml` | 💚 |
| Lakeflow Declarative Pipelines / DLT (serverless) | `resources/pipeline/*.yml` | 💚 (triggered) |
| Auto Loader (`cloudFiles`) + schema evolution + `_metadata` lineage | `bronze_resource.py`, `bronze_conglomerate.py` | 💚 |
| DLT streaming tables + materialized views | silver/gold transforms | 💚 |
| DLT expectations (`expect_all_or_drop`) | `expectation.py`, `silver_conglomerate.py` | 💚 |
| AUTO CDC / `apply_changes` (SCD1) + Change Data Feed (`readChangeFeed`) | `silver_resource.py` | 💚 |
| Liquid clustering (`cluster_by`) + informational PK/FK | conglomerate transforms, silver schemas | 💚 |
| Asset Bundles + Jobs (daily trigger) + databricks-connect tests + Photon/serverless | `databricks.yml`, `resources/jobs` | 💚 |

## Recommended additions 🚀 (free-first)

### Ingest (bronze) — incl. card #56
| Add | Cost | Note |
|---|---|---|
| **Auto Loader + `Trigger.AvailableNow`** (triggered hourly) | 💚 | Hourly cadence with **no always-on** compute — the cheapest streaming. |
| **Databricks Secret scopes** (`dbutils.secrets` / `${secrets/…}`) | 💚 | Hold the `api.minenergia.cl` token + any broker creds — not `.env`, not in code. |
| **Job-poller → UC Volume → Auto Loader** (skip Kafka for hourly) | 💚 | A small Job task pulls the API hourly and lands JSON in a Volume; Auto Loader ingests. **Avoids a managed-Kafka broker** (💲). See `card-56-bronze-landing-design.md`. |
| Auto Loader **`rescuedDataColumn`** + **schemaHints** | 💚 | Capture malformed rows; pin types for the `;`/comma-decimal CSVs. |
| **DLT Kafka source** (`read_kafka`) | 💲 (broker) | Only if a Kafka hop is mandated — the broker is the cost, not the Databricks side. |
| Auto Loader **file-notification mode** | 💲* | Adds a cloud queue (small); worth it only at high file volume. |
| **Lakeflow Connect** managed connectors | 💲 | Convenient but premium — skip for the PoC. |

### Bronze → Silver
| Add | Cost | Note |
|---|---|---|
| **`@dlt.append_flow`** (multiplex) | 💚 | Union solar+eólica / many stations into one streaming table without `union`. |
| **Quarantine pattern** (`expect_all` warn + bad-records table) | 💚 | Keep rejects instead of dropping silently. |
| **Expectations on every silver/gold** | 💚 | Only some tables have them today. |

### All tables (storage/perf)
| Add | Cost | Note |
|---|---|---|
| **`CLUSTER BY AUTO`** (auto liquid clustering) | 💚 | Extend clustering beyond conglomerate; no manual key choice. |
| **Deletion vectors** + **row tracking** | 💚 | Defaults for new Delta tables. |
| **Predictive Optimization** (auto OPTIMIZE/VACUUM) | 💲 | Saves query cost long-term but bills optimize compute — enable once data grows. |

### Gold / serving
| Add | Cost | Note |
|---|---|---|
| **AI/BI dashboards + Genie** over gold | 💚* | Included with DBSQL/serverless SQL; cost only while querying. |
| **DBSQL alerts** | 💲* | Runs a small scheduled query. |

### Quality & ops
| Add | Cost | Note |
|---|---|---|
| **DLT event log** queries + **system tables** (billing/lineage/query history) | 💚 | Free observability — query what you already have. |
| **Lakehouse Monitoring** | 💲 | Runs monitor jobs; adopt for the tables that matter most. |

### Governance (ties to prod-target hardening in `repo-improvements.md` §1.5)
| Add | Cost | Note |
|---|---|---|
| **Service principal `run_as`**, **tags**, **row filters / column masks**, group grants | 💚 | Real UC governance, no premium. |

## Suggested adoption order (free-first)
1. **Card-56 ingest**: Job-poller → UC Volume → **Auto Loader** DLT bronze, **triggered/AvailableNow hourly**, token in a **secret scope** — all 💚, no Kafka broker.
2. **Zero-cost wins**: `CLUSTER BY AUTO`, deletion vectors, expectations everywhere, quarantine, `append_flow`.
3. **Observability (free)**: system tables + DLT event log dashboards.
4. **Serving**: AI/BI dashboard over gold.
5. **Deliberate 💲**: Predictive Optimization, Lakehouse Monitoring, (managed Kafka only if mandated) — once data volume / SLAs justify them.
