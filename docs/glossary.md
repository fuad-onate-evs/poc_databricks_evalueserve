# Glossary — domain & technical terms

Shared vocabulary for **poc-databricks-evalueserve** (Chilean renewable-energy medallion on Databricks).
Canonical copy; also mirrored to the Trello "Glossary" card and Unity Catalog table/column comments.

## Domains (the two pipelines)
| Term | Pipeline | Meaning |
|---|---|---|
| **Resource** | `renewable_energy_chile` | **Plant-level Chilean** solar & wind **generation** (the operational data). |
| **Conglomerate** | `renewable_conglomerate_energy` | **Country-level / aggregated** renewable statistics (`Entity` × `Year`: Chile, World, South America…) for **cross-country comparison**. "Conglomerate" = the *aggregated, multi-entity* energy domain. |

## Generation "flavors" — Coordinador Eléctrico Nacional (CEN) terms
| Term | Code | Meaning |
|---|---|---|
| **Coordinado / Coordinated** | `coor` | **Programmed / scheduled** generation — what the grid operator *coordinated/dispatched* the plant to produce (the plan). |
| **Real** | `real` | **Actual measured** generation (what the plant *actually* produced). |
| **Reducciones / Reductions** | `reducciones` | **Curtailment** — renewable energy that was **cut / not injected** (forced reduction; e.g. grid congestion or oversupply). *Preliminary* = provisional figures, not yet settled. |
| **diff_real_coordinated** | — | `real − coordinated` — how far actual deviated from the plan. |

Conceptually: **real ≈ coordinated − reducciones − other deviations**.

## Energy types
- **Solar** — photovoltaic (PV).
- **Eólica / eolic** — wind.

## Architecture & data modeling
| Term | Meaning |
|---|---|
| **Medallion** | Layered design: **bronze** (raw ingested) → **silver** (cleansed + modeled) → **gold** (aggregated business metrics). |
| **Hub / Link / Satellite (sat)** | **Data Vault** modeling (silver): *hub* = business key (e.g. plant), *link* = relationship, *satellite* = time-varying measures. |
| **DLT / Lakeflow Declarative Pipelines** | Declarative transformation framework (the project's "T" — conceptually ≈ dbt). |
| **Auto Loader (`cloudFiles`)** | Incremental file ingestion into bronze. |
| **Unity Catalog (UC)** | Governance: catalogs → schemas → tables / **Volumes**. |
| **Volume** | UC-governed file storage (`/Volumes/<catalog>/<schema>/<volume>/…`). |
| **Asset Bundle (DAB)** | Infrastructure-as-code for Databricks resources (`databricks.yml`). |
| **Change Data Feed (CDF)** | Row-level change stream used by silver. |
| **SCD (Type 1/2)** | Slowly-Changing-Dimension handling in silver (`apply_changes`). |

## Key columns / dimensions
| Column | Meaning |
|---|---|
| **Entity** | Country/region (conglomerate domain). |
| **Year** | Year (conglomerate). |
| **Nombre / plant_name** | Plant identifier (resource domain). |
| **Fecha / Hora** | Date / hour of the measurement (raw). |
| **valor** | Measured value (Spanish decimal — comma — in bronze). |
| **value_date / record_date** | Normalized measurement timestamp (silver/gold). |
| **hash_key / diff_hash** | Surrogate keys (hashes) for Data-Vault / dedup. |

## Sources & organizations
| Term | Meaning |
|---|---|
| **CEN / Coordinador (Eléctrico Nacional)** | Chile's independent grid operator — source of `real` / `coordinado` / `reducciones`. |
| **DGF** | Departamento de Geofísica, Universidad de Chile — meteorological data source for card #56. |
| **Explorador Solar / Eólico** | DGF + Ministerio de Energía modeled solar/wind datasets (hourly, historical). |
| **api.minenergia.cl** | "API Energías Renovables" (DGF + Ministry) — primary programmatic source candidate (registration-gated). |

## Acronyms quick list
**CEN** Coordinador Eléctrico Nacional · **DGF** Depto. de Geofísica · **DLT** Delta Live Tables · **UC** Unity Catalog · **DAB** Databricks Asset Bundle · **CDF** Change Data Feed · **SCD** Slowly Changing Dimension · **GHI/DNI** Global/Direct solar irradiance · **PV** Photovoltaic.
