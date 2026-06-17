# Card #56 — DGF / U. de Chile data-acquisition discovery report

**Card:** [#56 — discovery a api or web scrapper…](https://trello.com/c/Ogj4qHZc) · **Source confirmed:** Departamento de Geofísica (DGF), Universidad de Chile (FCFM) · **Goal:** highest-frequency (aim per-minute) acquisition → Kafka → Databricks
**Method:** multi-source deep research — 5 angles, 21 sources fetched, 94 claims extracted, 25 adversarially verified (21 confirmed / 4 refuted). Pages fetched **2026-06-17**.

---

## ⚠️ Bottom line (read first)

**The DGF does NOT publish any documented, production-grade real-time REST API or sub-minute/sub-hourly streaming feed for its meteorological data.** The per-minute target **cannot be met from DGF's own published products as-is.** Concretely:

- DGF's flagship public datasets (**Explorador Solar**, **Explorador Eólico**) are **historical, modeled, gridded, HOURLY** products delivered by **manual bulk file download** — not streams.
- DGF's only "real-time"-flavored portal (**InfoMET**) explicitly says *"este NO es un sitio operacional"* and exposes **HTML only** → fragile scraping, with ToS/robustness risk.
- The single most promising programmatic lead is **`api.minenergia.cl`** (a DGF + Ministerio de Energía REST API), but its real-time capability is **unverified** and must be evaluated directly.
- True real-time station data in Chile flows through **DMC** (Dirección Meteorológica de Chile) and **DGA** — networks that CR2 aggregates — **not DGF itself.**

➡️ **Decision (2026-06-17): hourly accepted** → primary source = **`api.minenergia.cl`** (registration required; see §5). DMC/DGA remain the fallback for true real-time if ever needed.

---

## 1. Source comparison

| Source | Variables | Coverage / resolution | Cadence | Access | Auth | Format | Real-time? | Verdict |
|---|---|---|---|---|---|---|---|---|
| **`api.minenergia.cl`** — "API Energías Renovables" (DGF + MinEnergía) | solar radiation, wind speed, wave (oleaje) | national | **unknown** (likely hourly modeled) | **REST API** | registration | JSON/? | ❓ unverified | 🔍 **Evaluate first** — only DGF-tied REST API |
| **Explorador Solar** (`solar.minenergia.cl`) | GHI/DNI irradiance | 90 m grid, all Chile · **2004–2016** | **hourly** | bulk file download per location | none | CSV/PDF | ❌ historical/modeled | ⚠️ complementary/historical only |
| **Explorador Eólico** (`eolico.minenergia.cl`) | wind speed & direction, air density, turbine power | 1 km grid · WRF 2010/2015 + ReconClim 1980–2017 · 15 hub heights | **hourly** (daily for ReconClim) | "GENERAR INFORME" bulk export | none | CSV/PDF | ❌ historical/modeled | ⚠️ complementary/historical only |
| **DGF InfoMET** (`met.dgf.uchile.cl/tiempo`) | temp, wind, forecasts | viewer / framesets | "near real-time" (non-operational) | **HTML scraping only** | none | HTML | ⚠️ self-declared non-operational | 🟥 last-resort fallback (brittle + ToS risk) |
| **DGF LM-DGF / EDG station** | temp, humidity, pressure, GHI, wind, precip | single teaching station | 15-min averages | none documented (staff contact) | n/a | n/a | internal only | ❌ no programmatic access |
| **DMC Climatología** (`climatologia.meteochile.gob.cl`) | official met incl. radiation/wind | national network | station obs (hourly/10-min) | JSON menu endpoints | ? | JSON | ✅ official live network | 🔍 evaluate (true real-time path) |
| **CR2** (`cr2.cl`) | aggregates DMC/DGA/GHCN | 500+ stations | mixed (daily→hourly) | DB download + viewers | ? | CSV | partial | aggregator — real path = DMC/DGA |
| **redmeteo.cl** (`/api.html`) | private met network | private stations | real-time | **REST API** | ? | ? | ✅ | 🔍 evaluate (adjacent, non-DGF) |

## 2. Key verified findings (high confidence, 3-vote)

- **Explorador Solar** is a genuine DGF product (Molina/Falvey/Rondanelli, DGF/CR2; w/ MinEnergía), but a **historical modeled hourly** irradiance DB (2004–2016, 90 m), downloaded as a per-location file — *not* a stream. [Nature Sci. Reports](https://www.nature.com/articles/s41598-017-13761-x), [DGF news](http://www.dgf.uchile.cl/noticias/138956/explorador-solar-base-de-datos-sobre-radiacion-solar-en-chile)
- **Explorador Eólico** is a genuine DGF/FCFM product (WRF ARW v3.2, 1 km, hourly; hosted on `walker.dgf.uchile.cl`), download-only via "GENERAR INFORME" → CSV/PDF. [V2018 doc](https://eolico.minenergia.cl/downloads/docsEolicoV2018.pdf)
- **Both explorers** expose only **manual/bulk export**, **no documented REST API or streaming endpoint**.
- **InfoMET** self-declares non-operational and is **HTML/frameset only** — no API/CSV/JSON. [met.dgf.uchile.cl/tiempo](https://met.dgf.uchile.cl/tiempo/)
- **LM-DGF** is an **educational** lab; the EDG station logs 15-min averages but offers no programmatic retrieval. [LM-DGF](https://ingenieria.uchile.cl/investigacion/laboratorios/departamento-de-geofisica/laboratorio-de-meteorologia-lm-dgf)
- **CR2 is an aggregator** of DMC/DGA/GHCN — it does **not** operate DGF's stations; the live path runs through DMC/DGA. [cr2.cl](https://www.cr2.cl/datos-de-precipitacion/)

**Medium confidence (needs direct check):** `api.minenergia.cl` ("API Energías Renovables", contact **ernc@dgf.uchile.cl**) is the strongest REST-API lead tied to DGF, serving solar/wind/wave behind registration — but appears to serve **modeled climatology**, and its resolution/latency/auth/rate-limits/format are **unconfirmed**.

> Refuted & excluded (don't rely on): a "UdeC/DGEO station @5-min" claim (0–3); a mis-attribution of Explorador Solar away from DGF; an over-broad "CR2 is only daily/monthly".

## 3. Recommended acquisition architecture

Because DGF has no native per-minute stream, build the pipeline around a **poll-to-Kafka bridge** and treat DGF explorers as historical/complementary:

```
[live source]  →  scheduled poller (Databricks Job / Airflow / cron)  →  Kafka topic (JSON)  →  Databricks
   ^ evaluate, in priority order:                                                              ↘ Spark Structured Streaming (kafka source) — preferred
     1) api.minenergia.cl   2) DMC climatología   3) redmeteo.cl                               ↘ Auto Loader — if poller lands files in a UC Volume
```

- **Poller → Kafka:** a small producer queries the chosen endpoint at its native cadence and emits JSON to a topic. Cadence = the source's real resolution (per-minute only if the source supports it).
- **Databricks landing — two options:**
  - **Spark Structured Streaming from Kafka** — lowest latency, exactly-once via checkpointing. Best if true streaming is achieved.
  - **Auto Loader** — simpler, and **matches the repo's current bronze pattern**; use if the poller lands files in a UC Volume (see [card-56-bronze-landing-design.md](card-56-bronze-landing-design.md), Option A).
- **Fallback (last resort):** scrape InfoMET HTML — brittle against frameset changes and the site disclaims operational reliability; flag ToS/robustness risk before adopting.

## 4. Frequency decision — HOURLY ACCEPTED ✅ (2026-06-17)

The card asked for *"hourly or by minute."* DGF realistically offers **hourly** (modeled, via explorers / `api.minenergia.cl`); per-minute would require a non-DGF live network (DMC/DGA). **Decision: hourly is acceptable.** This makes `api.minenergia.cl` the **primary source** and simplifies the pipeline to a **scheduled hourly pull** — no always-on streaming required.

## 5. Primary source: `api.minenergia.cl` — status & registration

**Selected primary source:** `api.minenergia.cl` ("API Energías Renovables", DGF + Ministerio de Energía) — solar radiation, wind speed, wave; hourly. Endpoints sit **behind login** (`/api/` → `/login/`), so the concrete specs (paths, params, resolution, format, rate limits, auth scheme) can only be confirmed **after we have an account**.

**Registration** — web form at `https://api.minenergia.cl/register/` (Django). Fields: `first_name`, `last_name`, `email`, `institution_name`, `type` (Academia / Sector público / **Sector privado** / Org. sin fines de lucro / Persona natural / Otro), `username`, `password1/2`. **Protected by Google reCAPTCHA** → it **must be completed by a human in a browser** and cannot be automated. It is a *"Solicitar Registro"* (request) flow, so access may require approval. Recommended values:

| Field | Value |
|---|---|
| First / Last name | Fuad / Oñate |
| Email | `fuad.onate@evalueserve.com` |
| Institution | Evalueserve |
| Type | Sector privado |
| Username / Password | (your choice) |

**After approval + login**, next actions: probe `/api/` endpoints; confirm hourly resolution, JSON/CSV format and auth (session/token); then wire the **scheduled hourly poller → Kafka → Databricks** landing via **Auto Loader** (matches the current bronze pattern — Option A in `card-56-bronze-landing-design.md`; with hourly cadence, no continuous DLT is needed).

## 6. Fallbacks & open questions
- **Fallback sources** if `api.minenergia.cl` proves modeled-only/unsuitable: scheduled **Explorador** exports (hourly historical) or **DMC climatología** (`climatologia.meteochile.gob.cl`, official station obs).
- **Contact DGF** (ernc@dgf.uchile.cl / rgarreau@uchile.cl) re: an undocumented station feed if richer/live data is needed.
- `api.minenergia.cl` endpoint specs (resolution, auth, rate limits, format, ToS) remain **unconfirmed until login** — top item to close once registered.

## 7. Caveats
Negative ("no API") findings are non-exhaustive — verified against the relevant pages/docs, but a hidden undocumented endpoint can't be fully ruled out (hence: contact DGF). `api.minenergia.cl` specs are **unverified**. A few Explorador Eólico PDFs failed direct fetch and were corroborated via mirrors. The architecture in §3 is an engineering synthesis grounded in the verified facts, not an independently fact-checked claim.

## 8. Sources (primary)
- DGF explorers: [solar.minenergia.cl](https://solar.minenergia.cl) · [eolico.minenergia.cl](https://eolico.minenergia.cl/) · [Eólico V2018 PDF](https://eolico.minenergia.cl/downloads/docsEolicoV2018.pdf) · [DGF Explorador Solar news](http://www.dgf.uchile.cl/noticias/138956/explorador-solar-base-de-datos-sobre-radiacion-solar-en-chile)
- Peer-reviewed: [Molina et al., Nature Sci. Reports 2017](https://www.nature.com/articles/s41598-017-13761-x) ([PMC](https://www.ncbi.nlm.nih.gov/pmc/articles/PMC5665918/))
- DGF portals: [InfoMET](https://met.dgf.uchile.cl/tiempo/) · [LM-DGF](https://ingenieria.uchile.cl/investigacion/laboratorios/departamento-de-geofisica/laboratorio-de-meteorologia-lm-dgf)
- API lead: [api.minenergia.cl](https://api.minenergia.cl/)
- Adjacent/real-time: [DMC Climatología](https://climatologia.meteochile.gob.cl/) · [CR2 databases](https://www.cr2.cl/bases-de-datos/) · [CR2 precipitation](https://www.cr2.cl/datos-de-precipitacion/) · [redmeteo.cl API](https://redmeteo.cl/api.html) · [agrometR](https://github.com/ODES-Chile/agrometR)
