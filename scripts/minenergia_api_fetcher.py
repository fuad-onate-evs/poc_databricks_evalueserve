#!/usr/bin/env python3
"""Fetch weather/resource time-series from the official MinEnergía API.

The official "API Energías Renovables" (`api.minenergia.cl`, DGF + Ministerio de
Energía) serves **hourly, real-year** simulation data (1980–2017, WRF / MERRA-2) —
richer than the open Explorador's condensed typical-year.

Flow (reverse-engineered + validated against the live service):
    POST https://api.minenergia.cl/api/proxy?sim=<sim>
        Content-Type: application/json
        X-CSRFToken: <csrftoken cookie>
        X-Requested-With: XMLHttpRequest
        Referer: https://api.minenergia.cl/api/
        Cookie: sessionid=...; csrftoken=...
        body = the query JSON (action / period / export / variables / position)
    -> 200 {"url": ".../static/api/tempfiles/<id>/datos.csv"}
    -> GET that url (same session) to download the file.

Auth: a browser session cookie (the login form has no reCAPTCHA but programmatic
login is unreliable, so we reuse a captured session). Provide via env / .env:
    DGF_SESSIONID, DGF_CSRFTOKEN
Refresh them when the session expires (they are equivalent to a login — keep them
only in a gitignored .env).

Variables (27): wind vel/pow/vdir(+_offshore) · solar ghi/dni/glb/dif/dir/pv/cloud ·
met tempc/rh/pres/rho/evap/tsoil · marine tm0/pwave.

Usage:
    python scripts/minenergia_api_fetcher.py \
        --points-file data/renewable_plants.csv \
        --variables vel ghi --interval hour --start 2015-01-01 --end 2015-12-31 \
        --landing /Volumes/<catalog>/<bronze_schema>/lookup/minenergia/

    python scripts/minenergia_api_fetcher.py --lat -31.38 --lon -71.46 --name Canela \
        --variables vel --demo-print
"""
import argparse
import csv
import os
import sys

import requests

BASE = "https://api.minenergia.cl"
PROXY = BASE + "/api/proxy"
WIND_VARS = {"vel", "pow", "vdir", "vel_offshore", "vdir_offshore", "pow_offshore"}


def load_env(path=".env"):
    """Minimal .env loader (no dependency); real env vars win."""
    if os.path.exists(path):
        for line in open(path, encoding="utf-8"):
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())


def make_session():
    sid = os.environ.get("DGF_SESSIONID", "").strip()
    csrf = os.environ.get("DGF_CSRFTOKEN", "").strip()
    if not sid or not csrf:
        sys.exit("ERROR: set DGF_SESSIONID and DGF_CSRFTOKEN (browser session) in env/.env")
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0",
        "X-Requested-With": "XMLHttpRequest",
        "X-CSRFToken": csrf,
        "Referer": BASE + "/api/",
        "Origin": BASE,
        "Content-Type": "application/json; charset=utf-8",
        "Accept": "application/json, text/javascript, */*; q=0.01",
    })
    for k, v in {"sessionid": sid, "csrftoken": csrf}.items():
        s.cookies.set(k, v, domain="api.minenergia.cl")
    return s


def var_spec(vid, hgt):
    opts = {"label": vid, "stat": "default"}
    if vid in WIND_VARS:
        opts["recon"] = "off"
        opts["hgt"] = hgt
    return {"id": vid, "options": opts}


def read_points(args):
    """Return the position list [{label,type,lon,lat}, ...] from a file or a single point."""
    pts = []
    if args.points_file:
        for row in csv.DictReader(open(args.points_file, encoding="utf-8")):
            pts.append({"label": row.get("name") or row.get("label"),
                        "type": "point", "lon": float(row["lon"]), "lat": float(row["lat"])})
    elif args.lat is not None and args.lon is not None:
        pts.append({"label": args.name or "point", "type": "point",
                    "lon": float(args.lon), "lat": float(args.lat)})
    else:
        sys.exit("ERROR: provide --points-file or --lat/--lon")
    return pts


def build_payload(args, positions):
    return {
        "action": {"action": "series", "interval": args.interval, "stat": args.stat, "tmy": args.tmy},
        "period": {"start": args.start, "end": args.end},
        "export": {"label": "datos", "format": args.format},
        "variables": [var_spec(v, args.hgt) for v in args.variables],
        "position": positions,
    }


def fetch(session, payload, sim):
    """POST the query, then download the resulting file; return (bytes, filename)."""
    r = session.post(f"{PROXY}?sim={sim}", json=payload, timeout=120)
    r.raise_for_status()
    body = r.json()
    if body.get("_ERROR"):
        raise RuntimeError(f"API error code: {body['_ERROR']}")
    file_url = body["url"]
    data = session.get(file_url, timeout=120)
    data.raise_for_status()
    return data.content, file_url.rsplit("/", 1)[-1]


def main():
    ap = argparse.ArgumentParser(description="Fetch time-series from the official MinEnergía API.")
    src = ap.add_argument_group("site")
    src.add_argument("--points-file", help="CSV with columns name,lat,lon (+ optional resource)")
    src.add_argument("--lat", type=float)
    src.add_argument("--lon", type=float)
    src.add_argument("--name")
    q = ap.add_argument_group("query")
    q.add_argument("--variables", nargs="+", default=["vel"], help="variable ids, e.g. vel ghi dni")
    q.add_argument("--interval", default="hour", choices=["hour", "day", "month", "year"])
    q.add_argument("--stat", default="mean", choices=["mean", "median", "max", "min"])
    q.add_argument("--tmy", action="store_true", help="typical meteorological year (default: real year)")
    q.add_argument("--start", default="2015-01-01")
    q.add_argument("--end", default="2015-12-31")
    q.add_argument("--hgt", type=int, default=100, help="wind measurement height (m)")
    q.add_argument("--sim", default="2015", choices=["2015", "merra2"], help="2015=WRF, merra2=MERRA-2")
    q.add_argument("--format", default="csv", choices=["csv", "xlsx", "mat"])
    ap.add_argument("--landing", help="write the downloaded file here (dir); prints to stdout if omitted")
    ap.add_argument("--env-file", default=".env")
    args = ap.parse_args()

    load_env(args.env_file)
    session = make_session()
    positions = read_points(args)
    payload = build_payload(args, positions)
    content, fname = fetch(session, payload, args.sim)

    if args.landing:
        os.makedirs(args.landing, exist_ok=True)
        out = os.path.join(args.landing, "data." + args.format)
        with open(out, "wb") as f:
            f.write(content)
        print(f"OK  {len(positions)} site(s) × {len(args.variables)} var(s) "
              f"[{args.start}..{args.end} {args.interval}] -> {out} ({len(content)} bytes)")
    else:
        sys.stdout.write(content.decode("utf-8", "replace")[:2000])
    return 0


if __name__ == "__main__":
    _rc = main()
    if _rc:
        raise SystemExit(_rc)
