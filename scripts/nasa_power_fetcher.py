#!/usr/bin/env python3
"""Fetch historical hourly weather from NASA POWER for the plant coordinates.

NASA POWER (https://power.larc.nasa.gov) serves free, no-auth, historical hourly
weather by lat/lon (satellite + reanalysis). Its `community=RE` (Renewable Energy)
parameters cover the same fields the DMC measures — so this is the historical
counterpart of the real-time DMC station feed.

    GET https://power.larc.nasa.gov/api/temporal/hourly/point
        ?parameters=<...>&community=RE&latitude=<lat>&longitude=<lon>
        &start=YYYYMMDD&end=YYYYMMDD&format=JSON

Parameter -> DMC-equivalent field:
    ALLSKY_SFC_SW_DWN -> ghi (radiacionGlobalInst, W/m2)
    T2M               -> temperatura (C)
    RH2M              -> humedad (humedadRelativa, %)
    PS                -> presion (kPa)
    PRECTOTCORR       -> agua_caida (mm/h)
    WS10M / WD10M     -> viento_10m / direccion_viento (m/s, deg)
    WS50M             -> viento_50m (hub height, m/s)

Output: one long-format CSV row per plant × hour, landed for Auto Loader bronze.

Usage:
  python scripts/nasa_power_fetcher.py --points-file data/renewable_plants.csv \
      --start 2015-01-01 --end 2024-12-31 \
      --landing /Volumes/<catalog>/<bronze_schema>/lookup/nasa_power/
"""
import argparse
import csv
import datetime as dt
import os
import sys
import time
import urllib.request

BASE = "https://power.larc.nasa.gov/api/temporal/hourly/point"
# NASA POWER parameter -> output column (DMC-equivalent name)
PARAMS = {
    "ALLSKY_SFC_SW_DWN": "ghi",
    "T2M": "temperatura",
    "RH2M": "humedad",
    "PS": "presion",
    "PRECTOTCORR": "agua_caida",
    "WS10M": "viento_10m",
    "WD10M": "direccion_viento",
    "WS50M": "viento_50m",
}
OUT_COLS = ["plant", "resource", "lat", "lon", "momento",
            "ghi", "temperatura", "humedad", "presion", "agua_caida",
            "viento_10m", "direccion_viento", "viento_50m"]
FILL = -999.0  # NASA POWER's missing-value sentinel

# fallback plants if no --points-file (10 real Chilean solar/wind plants)
DEMO_PLANTS = [
    ("El Romero Solar", "solar", -29.05, -70.90), ("Cerro Dominador", "solar", -22.76, -69.47),
    ("Bolero", "solar", -22.90, -69.53), ("Luz del Norte", "solar", -27.30, -70.30),
    ("Quilapilun", "solar", -33.10, -70.72), ("Canela", "eolic", -31.38, -71.46),
    ("El Arrayan", "eolic", -31.70, -71.50), ("San Juan", "eolic", -27.80, -70.65),
    ("Sierra Gorda Este", "eolic", -22.90, -69.30), ("Negrete Cuel", "eolic", -37.60, -72.45),
]


def _download(url, attempts=4):
    last = None
    for i in range(attempts):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=120) as r:
                body = r.read().decode("utf-8")
            if body.strip().startswith("{"):
                return body
        except Exception as e:  # noqa: BLE001 - transient
            last = e
        time.sleep(2 * (i + 1))
    raise RuntimeError(f"download failed: {url[:90]} ({last})")


def _fetch_range(lat, lon, start, end):
    """One NASA POWER call for a range (<= 1 year); returns the parameter dict."""
    import json
    url = (f"{BASE}?parameters={','.join(PARAMS)}&community=RE"
           f"&latitude={lat}&longitude={lon}&start={start}&end={end}&format=JSON")
    return json.loads(_download(url))["properties"]["parameter"]


def fetch_point(name, resource, lat, lon, start, end):
    """Long-format rows for one plant, fetched in yearly chunks (the API caps the range)."""
    s = dt.datetime.strptime(start, "%Y%m%d").date()
    e = dt.datetime.strptime(end, "%Y%m%d").date()
    rows = []
    cur = s
    while cur <= e:
        hi = min(dt.date(cur.year, 12, 31), e)
        data = _fetch_range(lat, lon, cur.strftime("%Y%m%d"), hi.strftime("%Y%m%d"))
        for h in sorted(data[next(iter(PARAMS))].keys()):   # keys: YYYYMMDDHH
            vals = {col: data[p].get(h) for p, col in PARAMS.items()}
            if all(v is None or v == FILL for v in vals.values()):
                continue
            momento = f"{h[0:4]}-{h[4:6]}-{h[6:8]} {h[8:10]}:00:00"
            rows.append([name, resource, lat, lon, momento] +
                        [("" if (vals[c] is None or vals[c] == FILL) else vals[c]) for c in OUT_COLS[5:]])
        cur = dt.date(cur.year + 1, 1, 1)
    return rows


def main():
    ap = argparse.ArgumentParser(description="Fetch historical hourly weather from NASA POWER.")
    ap.add_argument("--points-file", help="CSV with columns name,resource,lat,lon")
    ap.add_argument("--start", default="2015-01-01", help="YYYY-MM-DD")
    ap.add_argument("--end", default=(dt.date.today() - dt.timedelta(days=1)).isoformat())
    ap.add_argument("--landing", help="write <landing>/data.csv (else print head to stdout)")
    args = ap.parse_args()

    plants = DEMO_PLANTS
    if args.points_file:
        plants = [(r.get("name") or r.get("plant"), r.get("resource", ""),
                   float(r["lat"]), float(r["lon"]))
                  for r in csv.DictReader(open(args.points_file, encoding="utf-8"))]

    start, end = args.start.replace("-", ""), args.end.replace("-", "")
    all_rows = []
    for name, resource, lat, lon in plants:
        try:
            rows = fetch_point(name, resource, lat, lon, start, end)
            all_rows += rows
            print(f"  OK  {name:22} {resource:5} {len(rows):6} hours")
        except Exception as e:  # noqa: BLE001
            print(f"  FAIL {name:21} {e}", file=sys.stderr)

    if args.landing:
        os.makedirs(args.landing, exist_ok=True)
        out = os.path.join(args.landing, "data.csv")
        with open(out, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(OUT_COLS)
            w.writerows(all_rows)
        print(f"OK  [{args.start}..{args.end}]  {len(all_rows)} rows -> {out}")
    else:
        print(",".join(OUT_COLS))
        for r in all_rows[:5]:
            print(",".join(str(x) for x in r))
    return 0


if __name__ == "__main__":
    _rc = main()
    if _rc:
        raise SystemExit(_rc)
