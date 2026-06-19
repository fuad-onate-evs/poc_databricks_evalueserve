#!/usr/bin/env python3
"""Publish DGF meteorological records to a Kafka topic — card #56, landing Option B.

Pairs with src/renewable_energy_chile/streaming/bronze_dgf_kafka.py on the Databricks
side: this producer puts JSON records on the topic, that DLT table reads them natively.

Two modes:
  --demo N        publish N synthetic hourly met records (runnable now, no DGF access —
                  great for exercising the local Kafka + the streaming bronze)
  --from-dir DIR  publish every JSON record found under DIR (e.g. the poller's landing
                  zone), so the real poller output can be replayed onto Kafka

Config (env):
  KAFKA_BOOTSTRAP   broker (default localhost:9092 — the local dev broker)
  KAFKA_TOPIC       topic  (default dgf_met)

Requires kafka-python:  pip install kafka-python
"""
from __future__ import annotations

import argparse
import datetime
import glob
import json
import math
import os
import sys

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "dgf_met")
STATIONS = [("CR-ANTOFAGASTA", -23.65, -70.40), ("CR-SANTIAGO", -33.45, -70.66)]


def _producer():
    try:
        from kafka import KafkaProducer
    except Exception:
        sys.exit("kafka-python not installed — run: pip install kafka-python")
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        key_serializer=lambda k: (k or "").encode(),
        value_serializer=lambda v: json.dumps(v).encode(),
    )


def demo_records(n: int):
    """Plausible synthetic hourly met records (irradiance follows a daytime curve)."""
    base = datetime.datetime(2026, 6, 1, 0, 0, 0)
    for i in range(n):
        sid, lat, lon = STATIONS[i % len(STATIONS)]
        ts = base + datetime.timedelta(hours=i // len(STATIONS))
        hour = ts.hour
        ghi = round(max(0.0, 900 * math.sin(math.pi * (hour - 6) / 12)), 1) if 6 <= hour <= 18 else 0.0
        yield sid, {
            "station_id": sid,
            "station_name": sid.replace("CR-", "").title(),
            "timestamp": ts.isoformat(),
            "latitude": lat, "longitude": lon,
            "ghi": ghi, "dni": round(ghi * 1.1, 1),
            "wind_speed": round(3 + 2 * math.sin(i / 3), 1),
            "wind_dir": round((i * 37) % 360, 1),
            "temperature": round(14 + 8 * math.sin(math.pi * (hour - 6) / 12), 1),
        }


def dir_records(directory: str):
    for path in sorted(glob.glob(os.path.join(directory, "**", "*.json"), recursive=True)):
        try:
            data = json.load(open(path, encoding="utf-8"))
        except Exception as exc:
            print(f"skip {path}: {exc}", file=sys.stderr)
            continue
        for rec in (data if isinstance(data, list) else [data]):
            yield str(rec.get("station_id", "")), rec


def main() -> int:
    ap = argparse.ArgumentParser(description="DGF → Kafka producer (card #56)")
    ap.add_argument("--demo", type=int, metavar="N", help="publish N synthetic records")
    ap.add_argument("--from-dir", metavar="DIR", help="publish JSON records found under DIR")
    args = ap.parse_args()
    if bool(args.demo) == bool(args.from_dir):
        ap.error("choose exactly one of --demo N or --from-dir DIR")

    records = demo_records(args.demo) if args.demo else dir_records(args.from_dir)
    producer = _producer()
    n = 0
    for key, value in records:
        producer.send(TOPIC, key=key, value=value)
        n += 1
    producer.flush()
    print(f"published {n} records to topic '{TOPIC}' at {BOOTSTRAP}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
