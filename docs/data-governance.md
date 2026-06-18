# Data governance model

How the **poc-databricks-evalueserve** data is governed in Unity Catalog. Companion to the
live **`data_catalog`** notebook (catalog survey + tags + grants + glossary) and [`glossary.md`](glossary.md).

## 1. Catalog & schema structure
- **Catalogs:** `prod` (production) and `dev` per the bundle; in fuad's sandbox the writable **`workspace`** catalog is used (the `dev` catalog isn't accessible — deploy with `--var=dev_catalog=workspace`).
- **Schemas = domain × medallion layer:** `<domain>_<layer>_energy_chile`
  - **Domains:** `resource` (plant-level Chilean generation) · `conglomerate` (country-level aggregates).
  - **Layers:** `bronze` (raw) · `silver` (modeled) · `gold` (business metrics).
  - Dev schemas are prefixed `dev_<short_name>_` (per-user isolation).
- ⚠️ **Naming irregularity:** the conglomerate **gold** schema is `..._conglomerate_gold_energy` (**no `_chile` suffix**) — a known landmine (`TABLE_OR_VIEW_NOT_FOUND` if assumed). Tracked in [`repo-improvements.md`](repo-improvements.md).

## 2. Classification — tags
Every schema carries UC tags (applied via `ALTER SCHEMA … SET TAGS`, visible in `information_schema.schema_tags`):
| Tag | Values | Purpose |
|---|---|---|
| `domain` | resource / conglomerate | data-domain ownership & discovery |
| `layer` | bronze / silver / gold | medallion stage |
| `medallion` | true | marks pipeline-managed schemas |
| `data_classification` | synthetic-sample | **this sandbox is fake data** — would become `internal` / `confidential` for real feeds |

Future: tag columns `pii` to drive masking; tag gold tables `business_critical`.

## 3. Data dictionary — comments
Definitions live **with the data** as UC `COMMENT`s on tables/columns (e.g. *coordinado*, *real*, *reducciones*,
`diff_real_coordinated`), surfaced in Catalog Explorer and the `data_catalog` notebook (§2–3). Source of truth: [`glossary.md`](glossary.md).

## 4. Access control (RBAC)
- Grants are managed in UC (`SHOW GRANTS ON SCHEMA/TABLE …`).
- **Production hardening (recommended):** run jobs/pipelines as a **service principal** (`run_as`) and grant to **groups**, not individuals — the current bundle hardcodes a personal owner ([`repo-improvements.md`](repo-improvements.md) §1.5). Apply **least privilege** (READ to consumers, MODIFY to pipelines).

## 5. Lineage
Unity Catalog captures **table + column lineage automatically** across bronze→silver→gold. View it per table in **Catalog Explorer → Lineage** (the `system.access.table_lineage` table needs elevated privileges, unavailable on Free Edition).

## 6. Data quality & observability
- **In-pipeline:** DLT **expectations** (`expect_all_or_drop`) — extend to quarantine instead of drop.
- **Monitoring:** the **Medallion-Health** and **End-to-End** AI/BI dashboards (counts, freshness, the coordinated-SCD check, pipeline run states) + the DLT event log.

## 7. Sensitive data
None today (synthetic). For real feeds: add **column masks / row filters**, set `data_classification` appropriately, and tag `pii` columns.

## Governance artifacts
| Artifact | Where |
|---|---|
| Data-catalog + governance + glossary notebook | Databricks `/Users/fuad.onate@evalueserve.com/data_catalog` · repo `notebooks/data_catalog.sql` |
| Glossary | [`docs/glossary.md`](glossary.md) · Trello card · UC comments · Databricks `glossary` notebook |
| Sample-data explore/validate notebook | `notebooks/sample_data_queries.sql` |
| Improvement backlog (incl. governance hardening) | [`docs/repo-improvements.md`](repo-improvements.md) |
