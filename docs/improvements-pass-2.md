# Improvement backlog — pass 2 (data modeling, performance & cost)

Follow-up to `repo-improvements.md` (README/CI/config/code-quality). These come from a deeper read
of the DLT transforms and focus on **data-modeling correctness**, **performance**, and **cost**.
Verify the 🔴 items against a real pipeline run before acting — they change data semantics.

## 🔴 Correctness (verify — potential data bugs)
| # | Problem | Where | Fix |
|---|---|---|---|
| C.1 | **`silver_hub_coordinated` SCD key collapses the time series.** `apply_changes(keys=["hash_key"])` with `hash_key = sha2(Nombre)` keeps **one SCD1 row per plant name**, dropping all hourly history. Sibling hubs key on `["hash_key, plant_name, record_source, value_date]`. | [silver_resource.py:118-124](../src/renewable_energy_chile/transformations/silver_resource.py#L118-L124) | Key coordinated on the same 4-part tuple (incl. `value_date`) so each timestamp is retained. |
| C.2 | **`silver_sat_measure` projects duplicate column names** — `t1/t2/t3.plant_name` → three columns all named `plant_name` (same for `value_date`/`value`), then references `t1.*` aliases *after* the projection. Ambiguous/fragile; can fail to resolve. | [silver_resource.py:170-214](../src/renewable_energy_chile/transformations/silver_resource.py#L170-L214) | Alias each column distinctly in the first `select` (`real_name`/`reduction_name`/`coordinated_name`, etc.) before `withColumns`. |
| C.3 | **Inconsistent SCD keys across the three hubs** (same root cause as C.1). | silver_resource.py | One shared key convention for all hubs. |

## 🟠 Performance & cost
| # | Problem | Where | Fix |
|---|---|---|---|
| P.1 | **Pipelines run sequentially** — `pipeline_energy_chile` `depends_on` `pipeline_conglomerate` (code comments `#this should be in parallel`); the domains are independent. | [full_execution.job.yml:20-23](../resources/jobs/full_execution.job.yml#L20-L23) | Drop the `depends_on` → run in parallel; lower wall-clock (cheaper on serverless). |
| P.2 | **Dead Python UDFs.** `modules.py` defines `replace_comma`/`replace_N_char` (Python `@udf`, no codegen) but they are **unused**; the native comma→dot already lives in `string_transformation.change_comma_to_dot`. | [modules.py](../src/renewable_energy_chile/transformations/modules.py) | Delete `modules.py`; use native `regexp_replace` if ever needed. |
| P.3 | **Heavy `.distinct()` + triple FULL OUTER JOINs** in `silver_link_measure`/`silver_sat_measure` can fan out / skew. | silver_resource.py | Review join keys; broadcast small dim sides; minimize `distinct`. |
| P.4 | **Materialized-view full recompute** — confirm gold MVs refresh incrementally where the query allows; cluster gold by common filter columns. | gold transforms | Validate incremental refresh. |

## 🟡 Structure & quality
| # | Problem | Where | Fix |
|---|---|---|---|
| S.1 | **`sys.path.append('../../.')` import hacks** to reach `helper` — fragile across local/workspace. | silver_resource.py, `*_conglomerate.py` | Package helpers under the project (editable install already configured) and import normally. |

## 🟢 Data quality & ops
| # | Problem | Where | Fix |
|---|---|---|---|
| Q.1 | **Minimal expectations** (3 rules) with silent `expect_all_or_drop`. | [expectation.py](../src/renewable_energy_chile/transformations/expectation.py) | Add value-range / freshness / non-null-key rules; quarantine rejects to a side table. |
| Q.2 | **No job resilience** beyond email-on-failure. | full_execution.job.yml | Add `max_retries` + `timeout_seconds`; add tags for cost attribution. |

> The 🟠/🟡/🟢 items are low-risk; the 🔴 items change data semantics and need a reviewed pipeline run.
> Companion to `repo-improvements.md` and `databricks-features-medallion.md`.
