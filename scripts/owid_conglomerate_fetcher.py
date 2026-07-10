#!/usr/bin/env python3
"""Fetch the Conglomerate-domain source data from Our World in Data (OWID).

Downloads the five open OWID "grapher" CSVs that feed the conglomerate medallion
and renames/reorders their metric columns to the exact header contract the
existing bronze/silver expect (see silver_conglomerate.py). Each dataset is
written as data.csv into its own lookup sub-directory so the existing Auto Loader
bronze (bronze_conglomerate.py) picks it up unchanged.

Source: https://ourworldindata.org/energy  (OWID energy data, open, no auth).

Usage:
  # land into the Unity Catalog Volume the bronze reads from
  python scripts/owid_conglomerate_fetcher.py \
      --landing /Volumes/<catalog>/<bronze_schema>/lookup

  # local dry run
  python scripts/owid_conglomerate_fetcher.py --landing ./_owid_landing
"""
import argparse
import csv
import io
import os
import sys
import time
import urllib.request

OWID = "https://ourworldindata.org/grapher/{slug}.csv?v=1&csvType=full&useColumnShortNames=false"

# For each dataset: the lookup sub-directory (must match bronze_conglomerate.raw_path),
# a name-based map from the OWID source column -> the pipeline's column name, and the
# output order of those target columns. Output header is always: Entity,Year,<out...>
# (Code is dropped to match the existing data.csv contract exactly).
DATASETS = [
    {"slug": "hydropower-consumption",
     "subdir": "hydropower_consumption",
     "colmap": {"Hydropower": "Hydro_Generation"},
     "out": ["Hydro_Generation"]},
    {"slug": "installed-solar-pv-capacity",
     "subdir": "installed_solar",
     "colmap": {"Solar": "Solar_Capacity"},
     "out": ["Solar_Capacity"]},
    {"slug": "modern-renewable-energy-consumption",
     "subdir": "renewable_energy_consumption",
     "colmap": {"Hydropower": "Electricity_from_hydro",
                "Bioenergy and other renewables": "Geo_Biomass_Other",
                "Solar": "Solar_Generation",
                "Wind": "Wind_Generation"},
     "out": ["Electricity_from_hydro", "Geo_Biomass_Other", "Solar_Generation", "Wind_Generation"]},
    {"slug": "modern-renewable-prod",
     "subdir": "modern_renewable_prod",
     "colmap": {"Wind": "Electricity_from_wind",
                "Hydropower": "Electricity_from_hydro",
                "Solar": "Electricity_from_solar",
                "Bioenergy and other renewables": "Other_renewables_including_bioenergy"},
     "out": ["Electricity_from_wind", "Electricity_from_hydro",
             "Electricity_from_solar", "Other_renewables_including_bioenergy"]},
    {"slug": "share-electricity-renewables",
     "subdir": "share_electricity_renewable",
     "colmap": {"Renewables": "Renewables"},
     "out": ["Renewables"]},
]


def _download(url, attempts=3):
    """GET a URL, retrying transient failures."""
    last = None
    for i in range(attempts):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=60) as r:
                body = r.read().decode("utf-8")
            if body.strip():
                return body
        except Exception as e:  # noqa: BLE001 - transient network errors
            last = e
        time.sleep(1.5 * (i + 1))
    raise RuntimeError(f"download failed: {url} ({last})")


def fetch_dataset(ds, landing):
    """Download one OWID dataset, map columns by name and write data.csv."""
    rows = list(csv.reader(io.StringIO(_download(OWID.format(slug=ds["slug"])))))
    header = rows[0]
    idx = {name: i for i, name in enumerate(header)}
    for src in ["Entity", "Year", *ds["colmap"]]:
        if src not in idx:
            raise ValueError(f"{ds['slug']}: missing source column '{src}' in {header}")
    # target name -> source index
    src_for = {tgt: idx[src] for src, tgt in ds["colmap"].items()}
    out_header = ["Entity", "Year", *ds["out"]]
    take = [idx["Entity"], idx["Year"], *[src_for[t] for t in ds["out"]]]

    out_dir = os.path.join(landing, ds["subdir"])
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "data.csv")
    n = 0
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(out_header)
        for r in rows[1:]:
            if len(r) >= len(header):
                w.writerow([r[i] for i in take])
                n += 1
    return out_path, n, out_header


def main():
    ap = argparse.ArgumentParser(description="Fetch OWID conglomerate-domain source CSVs.")
    ap.add_argument("--landing", required=True,
                    help="landing root; each dataset lands as <landing>/<subdir>/data.csv "
                         "(point at /Volumes/<catalog>/<bronze_schema>/lookup)")
    ap.add_argument("--only", nargs="*", choices=[d["subdir"] for d in DATASETS],
                    help="restrict to these datasets (default: all five)")
    args = ap.parse_args()

    targets = [d for d in DATASETS if not args.only or d["subdir"] in args.only]
    rc = 0
    for ds in targets:
        try:
            path, n, header = fetch_dataset(ds, args.landing)
            print(f"  OK  {ds['subdir']:28} {n:6} rows -> {path}")
            print(f"      columns: {','.join(header)}")
        except Exception as e:  # noqa: BLE001
            print(f"  FAIL {ds['subdir']:27} {e}", file=sys.stderr)
            rc = 1
    return rc


if __name__ == "__main__":
    _rc = main()
    if _rc:
        raise SystemExit(_rc)
