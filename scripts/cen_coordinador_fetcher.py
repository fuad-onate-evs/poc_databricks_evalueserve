#!/usr/bin/env python3
"""Fetch per-plant generation (Resource domain) from the CEN / Coordinador Eléctrico.

Source: the Coordinador's public SIPUB v1 API. The generation of the national grid
is **public information by law** (Art. 72-8, Ley 20.936). The Coordinador's own web
frontend calls this API with a *public* `user_key` embedded in its page config; the
same key is used here (no registration/account needed).

    GET https://sipubv1.api.coordinador.cl/api/v1/<resource>/?<params>&user_key=<key>
        (send Referer: https://www.coordinador.cl/)

Endpoints used:
  - recursos/infotecnica/centrales                      -> plant catalog (nombre, tipo S/E/T/H/G)
  - recursos/desviacion_generacion_grupo_reporte        -> per-plant/day:
        generacion_programada (coordinado) · generacion_real (real) · desviacion (reducciones)

It produces the six CSVs the Resource-domain bronze reads (semicolon-separated),
matching the silver contract exactly:
    lookup/<solar|eolic>/real/data.csv          Nombre;Fecha;Hora;valor   (valor = generacion_real)
    lookup/<solar|eolic>/coor/data.csv          Nombre;Fecha;valor        (valor = generacion_programada)
    lookup/<solar|eolic>/reducciones/data.csv   Nombre;Fecha;valor        (valor = desviacion)

Note on granularity: the per-plant series is **daily**; the `real` CSV carries Hora=0
(the SCADA hourly-per-plant feed is a separate Qlik source). Only solar (tipo S) and
eolic (tipo E) plants are emitted; a name-keyword fallback classifies PMGD not found
in the catalog (PFV/FV/SOLAR -> solar, EOL/EOLIC -> eolic).

Usage:
    python scripts/cen_coordinador_fetcher.py --start 2024-06-01 --end 2024-06-07 \
        --landing /Volumes/<catalog>/<bronze_schema>/lookup
"""
import argparse
import csv
import datetime as dt
import os
import sys

import requests

BASE = "https://sipubv1.api.coordinador.cl/api/v1"
# Public key embedded in the Coordinador's own operación-real chart pages (GraficosConfig.user_key_sipub).
PUBLIC_KEY = "f3cdad2758436a0a2c2c1fec92853de7"
TIPO_RESOURCE = {"S": "solar", "E": "eolic"}
# flavor -> (lookup sub-dir, source field in desviacion_generacion_grupo_reporte)
FLAVORS = {
    "real": ("real", "generacion_real"),
    "coor": ("coor", "generacion_programada"),
    "reducciones": ("reducciones", "desviacion"),
}


def api_get(session, resource, params=""):
    url = f"{BASE}/{resource}/?{params + '&' if params else ''}user_key={session.key}"
    r = session.get(url, timeout=120)
    r.raise_for_status()
    d = r.json()
    return d.get("data", d) if isinstance(d, dict) else d


def classify(nombre, name2tipo):
    """Return 'solar' / 'eolic' / None for a plant name (catalog first, keyword fallback)."""
    key = (nombre or "").strip().upper()
    tipo = name2tipo.get(key)
    if tipo in TIPO_RESOURCE:
        return TIPO_RESOURCE[tipo]
    if any(t in key for t in (" PFV", "PFV ", " FV ", "FOTOVOLT", "SOLAR", " PV ")):
        return "solar"
    if any(t in key for t in ("EOLIC", "EÓLIC", "EOL ", " WIND")):
        return "eolic"
    return None


def main():
    ap = argparse.ArgumentParser(description="Fetch CEN/Coordinador per-plant generation (Resource domain).")
    ap.add_argument("--start", help="YYYY-MM-DD (default: 7 days ago)")
    ap.add_argument("--end", help="YYYY-MM-DD (default: yesterday)")
    ap.add_argument("--landing", required=True,
                    help="lookup root; writes <landing>/<solar|eolic>/<sub>/data.csv")
    ap.add_argument("--user-key", default=os.environ.get("CEN_USER_KEY", PUBLIC_KEY))
    args = ap.parse_args()

    today = dt.date.today()
    start = args.start or (today - dt.timedelta(days=7)).isoformat()
    end = args.end or (today - dt.timedelta(days=1)).isoformat()

    s = requests.Session()
    s.headers.update({"User-Agent": "Mozilla/5.0", "Referer": "https://www.coordinador.cl/"})
    s.key = args.user_key

    # plant catalog -> name -> tipo
    centrales = api_get(s, "recursos/infotecnica/centrales")
    name2tipo = {(c.get("nombre") or "").strip().upper(): c.get("tipo") for c in centrales}

    # per-plant/day generation
    rows = api_get(s, "recursos/desviacion_generacion_grupo_reporte",
                   f"fecha__gte={start}&fecha__lte={end}")

    # bucket rows by (resource, flavor)
    buckets = {(res, fl): [] for res in TIPO_RESOURCE.values() for fl in FLAVORS}
    skipped = 0
    for r in rows:
        res = classify(r.get("central_nombre"), name2tipo)
        if res is None:
            skipped += 1
            continue
        nombre = r.get("central_nombre")
        fecha = str(r.get("fecha"))[:10]
        anexo = r.get("llave_nombre_natural")   # NombreAnexoCoordinador
        for fl, (_, field) in FLAVORS.items():
            val = r.get(field)
            if val is None:
                continue
            buckets[(res, fl)].append((nombre, fecha, val, anexo))

    total = 0
    for (res, fl), recs in buckets.items():
        sub = FLAVORS[fl][0]
        out_dir = os.path.join(args.landing, res, sub)
        os.makedirs(out_dir, exist_ok=True)
        with open(os.path.join(out_dir, "data.csv"), "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f, delimiter=";")
            if fl == "real":
                w.writerow(["Nombre", "Fecha", "Hora", "valor", "NombreAnexoCoordinador"])
                for nombre, fecha, val, anexo in recs:
                    w.writerow([nombre, fecha, 0, val, anexo])   # daily value at hour 0
            else:
                w.writerow(["Nombre", "Fecha", "valor"])
                for nombre, fecha, val, anexo in recs:
                    w.writerow([nombre, fecha, val])
        total += len(recs)
        print(f"  {res}/{sub:11} {len(recs):6} rows")
    print(f"OK  [{start}..{end}]  {total} rows written, {skipped} non-solar/eolic rows skipped")
    return 0


if __name__ == "__main__":
    _rc = main()
    if _rc:
        raise SystemExit(_rc)
