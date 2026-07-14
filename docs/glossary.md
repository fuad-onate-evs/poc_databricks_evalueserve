# Data Glossary

Business and technical glossary for the renewable-energy medallion. See
[`data-contracts.md`](data-contracts.md) for per-step data quality contracts.

---

## Business terms

| Term | Meaning |
|---|---|
| **ERNC** | Energías Renovables No Convencionales — non-conventional renewable energy (solar, wind, etc.). |
| **Coordinado** | Generation **programmed / scheduled** by the Coordinador for a plant (`generacion_programada`). |
| **Real** | Generation **actually measured** for a plant (`generacion_real`). |
| **Reducciones** | Curtailment / forced reduction of generation — here the **deviation** (`desviacion` = programmed − real). |
| **PMGD** | Pequeños Medios de Generación Distribuida — small distributed generation (often rooftop/PV). |
| **NombreAnexoCoordinador** | The Coordinador's registry key for a plant (`llave_nombre_natural`). |
| **CEN / Coordinador** | Coordinador Eléctrico Nacional — operator of the Chilean grid; owner of generation data. |
| **DGF** | Departamento de Geofísica, U. de Chile — owner of the weather/resource (irradiance, wind) data. |
| **OWID** | Our World in Data — source of country-level energy statistics. |
| **TMY** | Typical Meteorological Year — a modeled "average year" (the DGF Explorador product). |
| **GHI / DNI** | Global Horizontal / Direct Normal Irradiance (solar resource, kWh/m²/day). |
| **TWh / MW** | Terawatt-hour (energy) / Megawatt (power). |

## Architecture terms

| Term | Meaning |
|---|---|
| **Medallion** | Layered architecture: **bronze** (raw) → **silver** (clean/modeled) → **gold** (analytics). |
| **Auto Loader** | Databricks incremental file ingestion (`cloudFiles`) from a UC Volume into bronze. |
| **DLT** | Delta Live Tables / Lakeflow Declarative Pipelines — the pipeline framework. |
| **Data Vault** | Silver modeling for the Resource domain: **hub** (business keys) · **link** (relationships) · **sat** (attributes/measures). |
| **Expectation** | A DLT data-quality rule (`@dlt.expect_*`) that warns, drops, or fails on bad rows. |
| **UC Volume** | Unity Catalog managed storage where fetchers land source files. |

---

## Table dictionary

### Weather domain (`weather_*`, in the `renewable_*` schemas)
| Table | Grain | Key columns |
|---|---|---|
| `weather_bronze` | raw fetch | `data` (nested), `plant`, `resource`, `loaded_at` |
| `weather_silver` | plant (latest) | `plant`, `resource`, `lat`, `lon`, `ghi`, `dni`, `wind_ms` |
| `weather_gold_resource_kpi` | plant | `plant`, `resource`, `metric`, `rank`, `tier` (A/B/C) |

### Resource domain (`renewable_energy_chile`)
| Table | Grain | Key columns |
|---|---|---|
| `bronze_{coordinated,real,reductions_preliminary}_{solar,eolic}` | plant × date (× hour for `real`) | `Nombre`, `Fecha`, `Hora`, `valor`, `NombreAnexoCoordinador`, `resource` |
| `silver_hub_plant` / `_coordinated` / `_reductions` | plant | `hash_key`, `plant_name`, `value_date`, `value`, `record_source`, `resource` |
| `silver_link_measure` | plant relationships | `hash_key`, `plant_key`, `coordinated_key`, `reductions_key` |
| `silver_sat_measure` | plant × date | `hash_measure_key`, `real_name`, `coordinated_name`, `reduction_name`, `record_date`, `plant_value`, `coordinated_value`, `reduction_value` |
| `gold_{daily,weekly,monthly}_measure` | plant × period | `hash_link_key`, `date`, `avg_real`, `avg_coordinated`, `avg_reductions`, `diff_real_coordinated` |
| `gold_different_real_coordinated` | plant × date | `hash_link_key`, `date`, `value`, `porcent` |

### Conglomerate domain (`renewable_conglomerate_energy`)
| Table | Grain | Key columns |
|---|---|---|
| `bronze_hydropower_consumption` | country × year | `Entity`, `Year`, `Electricity_from_hydro` |
| `bronze_installed_solar_pv_capacity` | country × year | `Entity`, `Year`, `Solar_Capacity` |
| `bronze_modern_renewable_energy_consumption` | country × year | `Entity`, `Year`, `Electricity_from_hydro`, `Geo_Biomass_Other`, `Solar_Generation`, `Wind_Generation` |
| `bronze_modern_renewable_prod` | country × year | `Entity`, `Year`, `Electricity_from_wind/_hydro/_solar`, `Other_renewables_including_bioenergy` |
| `bronze_share_electricity_renewable` | country × year | `Entity`, `Year`, `Renewables` |
| `silver_dim_country` | country | `hash_key`, `entity` |
| `silver_fact_energy_consumption` | country × year | `hash_key`, `year`, `electricity_from_hydro`, `geo_biomass_other`, `solar_generation`, `wind_generation`, `hydro_generation` |
| `silver_fact_energy_production` | country × year | `hash_key`, `year`, `solar_capacity`, `electricity_from_wind/_hydro/_solar`, `other_renewables_including_bioenergy`, `renewables` |
| `gold_increase_consumption` / `gold_increase_production` | country × year | `hash_key`, `year`, `increase_*` |
| `gold_different_renewable_again_chile` | year | `year`, `chile_vs_latam`, `chile_vs_world` |

---

## Data sources → domains
| Domain | Owner / source | Access |
|---|---|---|
| Weather (resource/climate) | DGF / MinEnergía | open Explorador `python-router`; official API `/api/proxy` (session) |
| Resource (generation `valor`) | CEN / Coordinador | SIPUB v1 API, public `user_key` (data public by law) |
| Conglomerate (country) | OWID | open grapher CSVs |
