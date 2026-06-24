#!/usr/bin/env python3
"""Fetch REAL DGF (U. de Chile Geophysics) Explorador data — no auth required.

The Explorador Solar / Eolico Angular apps call an OPEN backend (no login, no token):

    GET https://<host>/python-router/<URL-encoded JSON>
    JSON = {"tipo": <op>, "datos": {"lat": .., "lon": .., ...}}

  - solar  (solar.minenergia.cl):  datos = {lat, lon}
  - eolic  (eolico.minenergia.cl): datos = {lat, lon, modelo: {"value": M, "recon": M}}
                                    where M in {recon (ReconClim), wrf2010, wrf2015}

ops:
  VistaRapida          -> annual + monthly means (GHI/DNI/diffuse/temp/wind/cloud ...)
  ExploracionCompleta  -> full series: SH (24h diurnal), SM (monthly), SA (annual)

Each point is landed as ONE JSON record (JSON Lines) in the bronze landing zone, so
the DGF Auto Loader bronze (src/renewable_energy_chile/streaming/bronze_dgf_autoloader.py)
ingests it. This gives REAL DGF resource data with NO dependency on the gated
api.minenergia.cl account.

NOTE: Explorador data is climatological typical-year (DGF model ~2004-2016) —
historical / typical, NOT live real-time.

Config (env or .env):
  DGF_LANDING        output dir / UC Volume path (default ./_dgf_landing)
  DGF_EOLIC_MODELO   wind model string sent in datos.modelo (set once confirmed)

Usage:
  python scripts/dgf_explorador_fetcher.py --resource solar --lat -33.45 --lon -70.66 --name Santiago
  python scripts/dgf_explorador_fetcher.py --resource solar --op ExploracionCompleta --points-file plants.csv
  python scripts/dgf_explorador_fetcher.py --resource eolic --lat -23.65 --lon -70.40 --modelo <model>
  python scripts/dgf_explorador_fetcher.py --demo        # a few northern-Chile solar points
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:  # pragma: no cover
    pass

HOSTS = {"solar": "https://solar.minenergia.cl", "eolic": "https://eolico.minenergia.cl"}
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/124.0 Safari/537.36"
EOLIC_MODELO = os.environ.get("DGF_EOLIC_MODELO", "recon")  # recon (ReconClim) | wrf2010 | wrf2015
# A few high-irradiance northern-Chile points (+ Santiago) for --demo.
DEMO_POINTS = [
    ("Antofagasta", -23.65, -70.40),
    ("Calama", -22.46, -68.93),
    ("Santiago", -33.45, -70.66),
]


def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT})
    retry = Retry(total=3, backoff_factor=0.5, status_forcelist=(429, 502, 503, 504),
                  allowed_methods=frozenset(["GET"]))
    s.mount("https://", HTTPAdapter(max_retries=retry))
    return s


def fetch_point(session, resource, op, lat, lon, name, modelo):
    """Call the Explorador python-router for one point; return an enriched record."""
    host = HOSTS[resource]
    datos = {"lat": lat, "lon": lon}
    if resource == "eolic":
        # the wind backend needs a {value, recon} object; the model name fills both
        datos["modelo"] = {"value": modelo, "recon": modelo}
    payload = {"tipo": op, "datos": datos}
    url = f"{host}/python-router/" + urllib.parse.quote(json.dumps(payload))
    r = session.get(url, headers={"Referer": f"{host}/"}, timeout=40)
    r.raise_for_status()
    body = r.json()  # Content-Type is text/html but the body is JSON
    return {
        "resource": resource,
        "op": op,
        "name": name,
        "lat": lat,
        "lon": lon,
        "modelo": modelo if resource == "eolic" else None,
        "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
        "server_status": body.get("_serverStatus"),
        "data": body.get("_serverData"),
    }


def land(records, landing: Path, resource: str, op: str) -> Path:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S_%fZ")
    landing.mkdir(parents=True, exist_ok=True)
    out = landing / f"dgf_{resource}_{op}_{ts}.jsonl"
    with out.open("w", encoding="utf-8") as fh:
        for rec in records:
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
    return out


def read_points(path: str):
    """Read points from CSV (name,lat,lon[,resource]) or a JSON list of objects/rows.

    Yields (name, lat, lon, resource); resource is None when the row doesn't set it.
    """
    if path.endswith(".json"):
        for row in json.load(open(path, encoding="utf-8")):
            if isinstance(row, dict):
                yield row.get("name", ""), float(row["lat"]), float(row["lon"]), row.get("resource")
            else:
                yield row[0], float(row[1]), float(row[2]), (row[3] if len(row) > 3 else None)
        return
    with open(path, encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            yield row.get("name", ""), float(row["lat"]), float(row["lon"]), row.get("resource")


def main() -> int:
    ap = argparse.ArgumentParser(description="DGF Explorador fetcher (no auth) — card #56")
    ap.add_argument("--resource", choices=["solar", "eolic"], default="solar")
    ap.add_argument("--op", choices=["VistaRapida", "ExploracionCompleta"], default="VistaRapida")
    ap.add_argument("--lat", type=float)
    ap.add_argument("--lon", type=float)
    ap.add_argument("--name", default="")
    ap.add_argument("--points-file", help="CSV (name,lat,lon) or JSON list")
    ap.add_argument("--modelo", default=EOLIC_MODELO, choices=["recon", "wrf2010", "wrf2015"],
                    help="wind model (eolic only): recon (ReconClim) | wrf2010 | wrf2015")
    ap.add_argument("--demo", action="store_true", help="fetch a few northern-Chile solar points")
    ap.add_argument("--dry-run", action="store_true", help="fetch + report, write nothing")
    args = ap.parse_args()

    if args.demo:
        points = [(n, la, lo, None) for n, la, lo in DEMO_POINTS]
    elif args.points_file:
        points = list(read_points(args.points_file))
    elif args.lat is not None and args.lon is not None:
        points = [(args.name, args.lat, args.lon, None)]
    else:
        ap.error("provide --lat/--lon, --points-file, or --demo")

    session = make_session()
    records, failures = [], 0
    for name, lat, lon, row_res in points:
        resource = (row_res or args.resource).strip().lower()  # per-row resource wins
        if resource == "eolic" and not args.modelo:
            print(f"  SKIP {name}: eolic needs --modelo (or DGF_EOLIC_MODELO)", file=sys.stderr)
            failures += 1
            continue
        try:
            rec = fetch_point(session, resource, args.op, lat, lon, name, args.modelo)
            ok = rec["server_status"] == "ok"
            failures += 0 if ok else 1
            keys = list((rec["data"] or {}).keys())[:6] if ok else rec["data"]
            print(f"  {resource}/{args.op} {name or ''} ({lat},{lon}) -> {rec['server_status']} {keys}")
            records.append(rec)
        except Exception as exc:
            failures += 1
            print(f"  ERROR {name} ({lat},{lon}): {exc}", file=sys.stderr)

    if not args.dry_run and records:
        landing = Path(os.environ.get("DGF_LANDING", "./_dgf_landing"))
        label = "plants" if args.points_file else args.resource
        print(f"landed {len(records)} record(s) -> {land(records, landing, label, args.op)}")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
