#!/usr/bin/env python3
"""Fetch real-time weather from DMC (Dirección Meteorológica de Chile) EMA stations.

The DMC "Servicios Climáticos" API serves the last ~12 hours of measured station data at
sub-hourly (minute-level) cadence — the real-time counterpart of the NASA POWER historical
feed (same fields). Access needs a free DMC account (usuario + token).

    GET https://climatologia.meteochile.gob.cl/application/servicios/getDatosRecientesEma/<code>
        ?usuario=<email>&token=<token>          (send Referer: https://climatologia.meteochile.gob.cl/)

Response: datosEstaciones.estacion (metadata: nombreEstacion, latitud, longitud) +
datosEstaciones.datos[] (records with `momento` + fields, values suffixed with units).

Field -> output column (aligned with the NASA POWER historical feed):
    radiacionGlobalInst -> ghi            temperatura        -> temperatura
    humedadRelativa     -> humedad        presionEstacion    -> presion
    aguaCaida24Horas    -> agua_caida     fuerzaDelViento    -> viento
    direccionDelViento  -> direccion_viento

Auth (env / .env): DMC_USUARIO, DMC_TOKEN.

Usage:
  python scripts/dmc_ema_fetcher.py --landing /Volumes/<catalog>/<bronze_schema>/lookup/dmc/
  python scripts/dmc_ema_fetcher.py --stations 330020 220002 --demo-print
"""
import argparse
import csv
import os
import re
import sys
import time

import requests

BASE = "https://climatologia.meteochile.gob.cl/application/servicios/getDatosRecientesEma"
# EMA station codes near the plants (north -> south); the API returns each station's lat/lon.
DEFAULT_STATIONS = ["220002", "270001", "290004", "300024", "320041", "330020", "360011", "380013"]
FIELD_MAP = {
    "radiacionGlobalInst": "ghi", "temperatura": "temperatura", "humedadRelativa": "humedad",
    "presionEstacion": "presion", "aguaCaida24Horas": "agua_caida",
    "fuerzaDelViento": "viento", "direccionDelViento": "direccion_viento",
}
OUT_COLS = ["estacion", "nombre", "lat", "lon", "momento",
            "ghi", "temperatura", "humedad", "presion", "agua_caida", "viento", "direccion_viento"]


def num(v):
    """DMC values are unit-suffixed strings ('10.1 °C', '76.78'); return the leading float or ''."""
    if v is None:
        return ""
    m = re.search(r"-?\d+\.?\d*", str(v))
    return m.group(0) if m else ""


def fetch_station(session, code, usuario, token):
    url = f"{BASE}/{code}?usuario={usuario}&token={token}"
    last = None
    for i in range(3):
        try:
            r = session.get(url, timeout=40)
            r.raise_for_status()
            j = r.json()
            de = j.get("datosEstaciones", {})
            est, datos = de.get("estacion", {}), de.get("datos", [])
            if est.get("mensaje") or "bloque" in str(j.get("mensaje", "")).lower():
                raise RuntimeError(j.get("mensaje") or est.get("mensaje"))
            rows = []
            for d in datos:
                rows.append([code, est.get("nombreEstacion"), est.get("latitud"), est.get("longitud"),
                             d.get("momento")] + [num(d.get(src)) for src in FIELD_MAP])
            return est, rows
        except Exception as e:  # noqa: BLE001 - transient / outage / block
            last = e
            time.sleep(3 * (i + 1))
    raise RuntimeError(f"station {code} failed: {last}")


def load_env(path=".env"):
    if os.path.exists(path):
        for line in open(path, encoding="utf-8"):
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())


def main():
    ap = argparse.ArgumentParser(description="Fetch real-time DMC EMA station weather.")
    ap.add_argument("--stations", nargs="+", default=DEFAULT_STATIONS, help="EMA station codes")
    ap.add_argument("--landing", help="write <landing>/data.csv (else print to stdout)")
    ap.add_argument("--env-file", default=".env")
    args = ap.parse_args()

    load_env(args.env_file)
    usuario, token = os.environ.get("DMC_USUARIO", ""), os.environ.get("DMC_TOKEN", "")
    if not usuario or not token:
        sys.exit("ERROR: set DMC_USUARIO and DMC_TOKEN in env/.env")

    s = requests.Session()
    s.headers.update({"User-Agent": "Mozilla/5.0", "Referer": "https://climatologia.meteochile.gob.cl/"})
    all_rows = []
    for code in args.stations:
        try:
            est, rows = fetch_station(s, code, usuario, token)
            all_rows += rows
            print(f"  OK  {code}  {est.get('nombreEstacion','?'):28} {len(rows):5} records")
        except Exception as e:  # noqa: BLE001
            print(f"  FAIL {code}  {e}", file=sys.stderr)

    if args.landing:
        os.makedirs(args.landing, exist_ok=True)
        out = os.path.join(args.landing, "data.csv")
        with open(out, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(OUT_COLS)
            w.writerows(all_rows)
        print(f"OK  {len(args.stations)} stations, {len(all_rows)} records -> {out}")
    else:
        print(",".join(OUT_COLS))
        for r in all_rows[:5]:
            print(",".join(str(x) for x in r))
    return 0 if all_rows else 1


if __name__ == "__main__":
    _rc = main()
    if _rc:
        raise SystemExit(_rc)
