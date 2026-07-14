# Data Contracts & Governance

Governance for the renewable-energy medallion is **Databricks-native**: Unity Catalog for
lineage, tags and comments; **DLT expectations** for in-pipeline data quality; Delta / UC
constraints for structural guarantees. This document is the **contract for every step** of
the medallion, across the three domains (Weather · Resource · Conglomerate).

Legend: ✅ = enforced in code today · ➕ = recommended expectation to add.

---

## Governance model

| Concern | Mechanism | Where |
|---|---|---|
| **Data quality / contracts** | DLT `@dlt.expect_*` (warn or drop) | silver (today) → bronze + gold (extending) |
| **Schema contract** | explicit `schema=` in `@dlt.table` / Auto Loader `schemaHints` | bronze |
| **Lineage** | Unity Catalog automatic (table + column) | all layers |
| **Glossary** | UC tags + column `COMMENT` + [`glossary.md`](glossary.md) | all layers |
| **Freshness / drift** | Lakehouse Monitoring (recommended) | gold |
| **Observability** | DLT event log (`event_log`) — expectation pass/fail per run | pipelines |

Expectation severities: `expect` (warn, keep row) · `expect_or_drop` (drop bad row) ·
`expect_all_or_drop` (drop if any rule fails) · `expect_or_fail` (abort the update).

---

## Weather domain (`weather_*`)

Source: DGF / MinEnergía (hourly resource — GHI/DNI/wind). Fetchers: `dgf_explorador_fetcher.py`,
`minenergia_api_fetcher.py`.

| Step | Table | Contract |
|---|---|---|
| **Bronze** | `weather_bronze` | Auto Loader (cloudFiles JSON) from the UC Volume; raw + file lineage. ➕ `expect valid_json: data IS NOT NULL`. |
| **Silver** | `weather_silver` | Flatten VR (ghi/dni/temp/wind), latest-per-plant dedup. ✅ `known_resource: resource IN ('solar','eolic')` · `valid_coords: lat BETWEEN -56 AND -17 AND lon BETWEEN -76 AND -66` · `has_metric: ghi IS NOT NULL OR wind_ms IS NOT NULL`. |
| **Gold** | `weather_gold_resource_kpi` | Per-plant KPI (metric, rank, A/B/C tier). ➕ `expect valid_tier: tier IN ('A','B','C')` · `non_negative_metric: metric >= 0` · `valid_rank: rank >= 1`. |

## Resource domain (`renewable_energy_chile`)

Source: CEN / Coordinador (per-plant generation). Fetcher: `cen_coordinador_fetcher.py`.
Silver = Data Vault (hub / link / sat).

| Step | Table(s) | Contract |
|---|---|---|
| **Bronze** | `bronze_{coordinated,real,reductions_preliminary}_{solar,eolic}` | Auto Loader (cloudFiles CSV, `;`-sep). **Column contract:** `Nombre`, `Fecha`, `valor` (+ `Hora`, `NombreAnexoCoordinador` for `real`); `resource` literal added. ➕ `schemaHints="valor DOUBLE, Fecha DATE"` + `expect has_value: valor IS NOT NULL` · `named_plant: Nombre IS NOT NULL`. |
| **Silver** | `silver_hub_plant` / `silver_hub_coordinated` / `silver_hub_reductions`, `silver_link_measure`, `silver_sat_measure` | Hash keys, plant-name normalization, `value_date`, comma→dot. ✅ `valid_planta_name: plant_name IS NOT NULL` · `valid_hash_key: length(hash_key)=64` · `recent_load_date: load_date > '2024-01-01'`. ➕ `numeric_value: value IS NULL OR CAST(value AS DOUBLE) IS NOT NULL`. |
| **Gold** | `gold_{daily,weekly,monthly}_measure`, `gold_different_real_coordinated` | avg real/coord/reductions + real-vs-coordinated diff. ➕ `expect valid_date: date IS NOT NULL` · `bounded_pct: porcent BETWEEN -1000 AND 1000`. |

## Conglomerate domain (`renewable_conglomerate_energy`)

Source: OWID (country stats). Fetcher: `owid_conglomerate_fetcher.py`.

| Step | Table(s) | Contract |
|---|---|---|
| **Bronze** | `bronze_hydropower_consumption`, `bronze_installed_solar_pv_capacity`, `bronze_modern_renewable_energy_consumption`, `bronze_modern_renewable_prod`, `bronze_share_electricity_renewable` | Auto Loader (cloudFiles CSV). **Column contract:** `Entity`, `Year` + metric columns (e.g. `Electricity_from_hydro`, `Solar_Generation`, `Solar_Capacity`, `Renewables`). ➕ `expect has_entity: Entity IS NOT NULL` · `valid_year: Year BETWEEN 1900 AND 2100`. |
| **Silver** | `silver_dim_country`, `silver_fact_energy_consumption`, `silver_fact_energy_production` | dim/fact, hashed keys. ✅ `valid_values: NOT Entity RLIKE '[0-9]'` (dim) · `valid_year: year < 2100` (facts). ➕ `non_negative_twh: solar_generation >= 0 OR solar_generation IS NULL`. |
| **Gold** | `gold_increase_consumption`, `gold_increase_production`, `gold_different_renewable_again_chile` | YoY increase + Chile-vs-LATAM/World. ➕ `expect valid_year: year IS NOT NULL`. |

---

## Enforcement & monitoring

- **In-pipeline:** expectations run every DLT update; results are queryable in the pipeline
  **event log** (`SELECT * FROM event_log(...) WHERE event_type='flow_progress'`), exposing
  rows passed/failed per rule.
- **Structural:** primary-key / not-null via UC `create_streaming_table(schema=...)` and Delta
  `NOT NULL` constraints (already used on the conglomerate silver facts).
- **Freshness / drift (recommended):** attach **Lakehouse Monitoring** to each gold table +
  a job-failure email alert, closing the loop once sources refresh on schedule.

Applying the ➕ rules is a code change per pipeline (respecting each domain's DLT API:
`import dlt` for Resource/Weather, `from pyspark import pipelines as dp` for Conglomerate)
followed by a pipeline full-refresh.
