# Card #56 — Landing high-frequency U. de Chile (DGF) data on Bronze

**Card:** [discovery a api or web scrapper to get the data in a frequency hourly or by minute](https://trello.com/c/Ogj4qHZc) · **List:** Backlog · **Due:** 2026-06-19
**Data flow:** `Universidad de Chile (DGF, Dept. de Geofísica) → Kafka → Databricks (bronze)`

> Companion to the data-source discovery report (see `docs/` — DGF acquisition options).
> This note covers the **Databricks-side landing**: how the new high-frequency stream
> should reach the **bronze** layer given the project's current architecture.

---

## 1. Current bronze architecture (as-is)

| Aspect | Today |
|---|---|
| Ingestion | **Auto Loader** (`spark.readStream.format("cloudFiles")`) on **CSV** files |
| Source location | **Unity Catalog Volumes**: `/Volumes/{catalog}/{bronze_schema}/lookup/.../` |
| Resource domain | solar/eólica × `{coor, real, reducciones}`, `;`-sep, Spanish decimals, `schemaEvolutionMode=addNewColumns` — [bronze_resource.py](../src/renewable_energy_chile/transformations/bronze_resource.py) |
| Conglomerate domain | `,`-sep, `addNewColumnsWithTypeWidening`, `cluster_by`, newer `pyspark.pipelines` API — [bronze_conglomerate.py](../src/renewable_conglomerate_energy/transformations/bronze_conglomerate.py) |
| Lineage | every row keeps `_metadata.file_path → file_path` and `file_modification_time → modification_date` |
| Pipelines | DLT, **serverless**, **triggered** (not continuous) — [renewables_energy_chile_etl.pipeline.yml](../resources/pipeline/renewables_energy_chile_etl.pipeline.yml) |
| Orchestration | **daily** job, conglomerate→resource sequential — [full_execution.job.yml](../resources/jobs/full_execution.job.yml) |
| Silver | `readChangeFeed` from bronze, **truncates to hourly**, Data-Vault hub/link/sat keyed on plant, uses `file_path` as `record_source` — [silver_resource.py](../src/renewable_energy_chile/transformations/silver_resource.py) |

**Tension with the assignment:** the bronze design is **file-based batch on a daily trigger**, and silver
collapses everything to **hourly**. Card #56 asks for **per-minute / hourly** data over **Kafka** — a streaming
cadence the current pipeline is not shaped for. The DGF data is also *meteorological* (irradiance, wind, temp per
**station**), not per-**plant** generation, so it lands as a **new bronze table feeding a new silver branch**, not a
union into the existing plant hubs.

---

## 2. Options to land the Kafka stream on bronze

### Option A — (poller) → UC Volume (landing zone) → Auto Loader  ✅ recommended (free + native; see §3)
A lightweight consumer (or Kafka Connect / Spark `foreachBatch` sink) writes micro-batch files
(JSON or CSV) into a **new** Volume path, e.g. `/Volumes/{catalog}/{bronze_schema}/lookup/dgf_met/<station>/`,
and a **new `@dlt.table`** ingests them with the *same* Auto Loader pattern as today.

- **Pros:** maximum consistency with existing bronze code; keeps `_metadata` lineage, schema evolution, and the
  triggered-pipeline model; **decouples** Kafka uptime from pipeline runtime (files are replayable → easy backfill);
  cheapest (no always-on cluster, no broker). Hourly met data is low-volume, so a 1-file-per-hour cadence is trivial.
- **Cons:** one extra hop (Kafka→files) adds ≈ the flush interval of latency; risk of **small-file** sprawl →
  mitigate by batching (1 file/min or /5-min) + periodic `OPTIMIZE`.

### Option B — Kafka → DLT streaming table directly (`readStream.format("kafka")`)
A new bronze `@dlt.table` reads Kafka directly; run the pipeline in **continuous** mode.

- **Pros:** lowest latency, no intermediate files, medallion-native streaming.
- **Cons:** new pattern (no Volumes/Auto Loader); needs Kafka **connection secrets** in the pipeline; **always-on**
  compute cost; loses the `file_path` lineage convention silver relies on (must substitute `topic/partition/offset`
  as `record_source`); changes orchestration away from the daily job.

### Option C — Hybrid (separate continuous pipeline for the stream)
Keep the daily file pipeline untouched; add a **dedicated** continuous DLT pipeline (or a standalone Structured
Streaming job) for the DGF Kafka source → its own bronze Delta table; downstream silver unions/joins as needed.

- **Pros:** zero disturbance to the working batch pipeline; isolates always-on cost to just the stream.
- **Cons:** two pipelines to operate; more orchestration surface.

---

## 3. Recommendation — free + Databricks-native, hourly

With **hourly** as the agreed cadence (see the discovery report) and a goal of **maximising
Databricks-native features at minimum cost**, the recommended landing is **Option A made fully
native and serverless** — and, for hourly data, **drop the managed Kafka broker** (it is avoidable cost):

> **Databricks Job (poller) → UC Volume (JSON) → Auto Loader DLT bronze, run triggered / `Trigger.AvailableNow` hourly.**

Why this is both the cheapest *and* the most Databricks-native:
- **No managed Kafka broker** (💲) — for hourly volume a broker adds cost + ops with no benefit. A small **Databricks
  Job** task polls `api.minenergia.cl` hourly and lands JSON in a **UC Volume**. *(If a Kafka hop is mandated by the
  brief, the broker is the only paid part — read it natively, see "If Kafka is required" below.)*
- **No always-on compute** — the DLT pipeline runs **triggered / `Trigger.AvailableNow`** on an hourly schedule, not continuous.
- **All included features (💚):** Auto Loader, DLT streaming tables + expectations, **secret scopes**, schema evolution,
  `_metadata` lineage, replayable files for easy backfill — consistent with the existing bronze code.

See [`databricks-features-medallion.md`](databricks-features-medallion.md) for the broader free-first feature map.

### Concrete shape (recommended)
- **Poller:** a **Databricks Job** Python task (serverless or smallest single-node) calls `api.minenergia.cl` hourly;
  the API **token lives in a Databricks secret scope** (`${secrets/…}`), not `.env`.
- **Landing zone:** new Volume `dgf_met` under the resource bronze schema (extend [volume.yml](../resources/volume.yml));
  lay out files by `station/date/hour`.
- **Format:** **JSON** (nested met readings + event timestamp) with `cloudFiles.inferColumnTypes`,
  `schemaEvolutionMode=addNewColumns`, and `rescuedDataColumn` for malformed rows.
- **Bronze table:** new `@dlt.table bronze_dgf_met()` mirroring the existing Auto Loader functions — select `*`,
  `_metadata.file_path AS file_path`, `_metadata.file_modification_time AS modification_date`, literal `source='dgf'`;
  keep the payload **event-time** for hourly bucketing. Use **`@dlt.append_flow`** if multiplexing multiple stations.
- **Cadence:** pipeline **triggered / `AvailableNow`** on an **hourly** job schedule (cheap; no always-on).
- **DQ:** extend [expectation.py](../src/renewable_energy_chile/transformations/expectation.py) with met rules
  (non-null station, value ranges, recent event-time); **quarantine** rejects rather than drop.
- **Perf (free):** `CLUSTER BY AUTO` + deletion vectors on the new tables.

### If Kafka is required (Option B, native)
If the `→ Kafka →` hop must stay, read it the Databricks-native way: a bronze `@dlt.table` with a **Kafka source**
(`read_kafka` / `readStream.format("kafka")`), broker creds in a **secret scope**, pipeline **triggered hourly** (not
continuous). The **broker is the only added cost (💲)**; the Databricks side stays free/included. Substitute
`topic/partition/offset` for the `file_path` lineage.

---

## 4. Open questions / dependencies
- **Source specifics** (endpoints, real frequency, format, auth) — pending the DGF data-source discovery report.
- **Kafka infra**: managed (Confluent / MSK / Event Hubs-Kafka) vs self-hosted? security (SASL/TLS)? topic design?
- **Per-minute cost**: continuous DLT vs frequent-triggered — confirm budget tolerance.
- **Silver modeling**: new met branch (station-level) vs mapping to the existing plant model — needs a follow-up card.
