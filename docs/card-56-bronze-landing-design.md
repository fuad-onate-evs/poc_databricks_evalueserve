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

### Option A — Kafka → UC Volume (landing zone) → Auto Loader  ✅ recommended for the PoC
A lightweight consumer (or Kafka Connect / Spark `foreachBatch` sink) writes micro-batch files
(JSON or CSV) into a **new** Volume path, e.g. `/Volumes/{catalog}/{bronze_schema}/lookup/dgf_met/<station>/`,
and a **new `@dlt.table`** ingests them with the *same* Auto Loader pattern as today.

- **Pros:** maximum consistency with existing bronze code; keeps `_metadata` lineage, schema evolution, and the
  triggered-pipeline model; **decouples** Kafka uptime from pipeline runtime (files are replayable → easy backfill);
  cheapest (no always-on cluster). Per-minute met data is low-volume, so a ~1-file-per-minute cadence is fine.
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

## 3. Recommendation

**Adopt Option A for the PoC**, isolated as a new bronze table + new Volume landing path, and add a clear migration
path to **Option B/continuous** if/when **sub-minute latency** becomes a hard requirement.

Rationale: it reuses the team's proven Auto Loader/Volume/DLT pattern, preserves lineage and schema-evolution
behaviour, keeps the stream **replayable** (critical for a PoC), and avoids always-on cost — while per-minute
meteorological volume is small enough that the file-hop latency is acceptable. Promote to Option B only when the
file-flush latency or small-file overhead is proven to be the bottleneck.

### Concrete shape (Option A)
- **Landing zone:** new Volume `dgf_met` under the resource bronze schema (extend [volume.yml](../resources/volume.yml));
  partition by `station/date/hour` to bound small files.
- **Format:** prefer **JSON** (carries nested met readings + event timestamp cleanly) with
  `cloudFiles.inferColumnTypes` + `schemaEvolutionMode=addNewColumns`.
- **Bronze table:** new `@dlt.table bronze_dgf_met()` in the resource pipeline mirroring the existing functions,
  selecting `*`, `_metadata.file_path AS file_path`, `_metadata.file_modification_time AS modification_date`, and a
  literal `source='dgf'`; keep the **event-time** field from the payload for downstream hourly/minute bucketing.
- **Cadence:** set the resource pipeline to **continuous**, or trigger the job every N minutes, for the high-freq table.
- **Kafka→Volume bridge:** a small poller/consumer (the discovery PoC script) producing to a topic + a sink writing
  batched files; or Kafka Connect with a cloud-storage sink mapped to the Volume's external location.
- **DQ:** extend [expectation.py](../src/renewable_energy_chile/transformations/expectation.py) with met-specific rules
  (non-null station, value ranges, recent event-time).

---

## 4. Open questions / dependencies
- **Source specifics** (endpoints, real frequency, format, auth) — pending the DGF data-source discovery report.
- **Kafka infra**: managed (Confluent / MSK / Event Hubs-Kafka) vs self-hosted? security (SASL/TLS)? topic design?
- **Per-minute cost**: continuous DLT vs frequent-triggered — confirm budget tolerance.
- **Silver modeling**: new met branch (station-level) vs mapping to the existing plant model — needs a follow-up card.
