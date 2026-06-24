# DGF ingestion — pipeline & Kafka code (card #56)

Two ready-to-activate ways to land the DGF (U. de Chile Geophysics) feed on **bronze**,
matching `docs/card-56-bronze-landing-design.md`. Neither is wired into the running
pipeline yet — the pipeline glob only includes `transformations/**`, so these files in
`streaming/` are inert until you activate them.

> **Real DGF data NOW — no account needed.** `scripts/dgf_explorador_fetcher.py` pulls
> genuine DGF **Explorador Solar/Eólico** series from the open `python-router` backend
> (no login) and lands them as JSONL into the bronze landing zone, feeding the same Auto
> Loader bronze below — so we get real data **without** waiting for the gated
> `api.minenergia.cl` account. (Explorador data is climatological typical-year, not live
> real-time.) Try it: `python scripts/dgf_explorador_fetcher.py --resource solar --demo`
> (or `--resource eolic`). The `dgf_poller.py` path stays for the gated `/api/` once approved.

| | File | When |
|---|---|---|
| **Option A** *(recommended, hourly)* | `bronze_dgf_autoloader.py` | poller lands JSON in a UC Volume → Auto Loader DLT bronze. Cheapest, replayable, no broker. |
| **Option B** *(streaming)* | `bronze_dgf_kafka.py` + `scripts/dgf_kafka_producer.py` | producer → Kafka topic → native `readStream.format("kafka")` bronze. For sub-minute / multi-consumer / replay. |

## Option A — activate
1. Add a `dgf` landing path to the Volume: extend `resources/volume.yml`, then point the
   poller at it (`DGF_LANDING=/Volumes/<catalog>/<bronze_schema>/lookup/dgf/`).
2. Include this folder in the pipeline: in
   `resources/pipeline/renewables_energy_chile_etl.pipeline.yml`, add a second glob
   `include: ../../src/renewable_energy_chile/streaming/bronze_dgf_autoloader.py`
   (or move the file into `transformations/`).
3. `databricks bundle deploy -t dev --var=dev_catalog=workspace` and run the pipeline.
4. Once the real `/api/` payload is confirmed, pin the explicit `schema=` with COMMENTs
   (template is in the file header) — same documented-columns pattern as the other bronze.

## Option B — activate
1. Stand up a broker (local dev: KRaft on `localhost:9092`, or a managed Kafka) and set
   the pipeline `configuration:` keys `kafka.bootstrap` / `kafka.topic` (broker creds via a
   **secret scope**, see the commented options in `bronze_dgf_kafka.py`).
2. Publish records:
   ```bash
   pip install kafka-python
   python scripts/dgf_kafka_producer.py --demo 48          # synthetic, runnable now
   # or replay the poller's landed files:
   python scripts/dgf_kafka_producer.py --from-dir ./_dgf_landing
   ```
3. Include `bronze_dgf_kafka.py` in the pipeline glob and run it in **continuous** mode
   (it needs always-on compute).

> Recommendation: for the agreed **hourly** cadence, prefer **Option A** — a managed broker
> adds cost + ops with no benefit. Keep Option B for when a true streaming source appears
> (e.g. DMC/DGA sub-minute station data).
