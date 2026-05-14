# GRT Reliability Tracker

Collector and early data tooling for a Grand River Transit reliability and delay prediction project.

The current phase stores raw GTFS-Realtime snapshots for later parsing, analysis, and model training. It polls GRT bus/LRT realtime feeds every 30 seconds, stores static GTFS ZIPs once per UTC day, and can upload all snapshots to Google Cloud Storage.

## What It Collects

Realtime feeds:

- Bus trip updates
- Bus vehicle positions
- LRT trip updates
- LRT vehicle positions

Static feeds:

- Bus static GTFS ZIP
- LRT static GTFS ZIP

Default output layout:

```text
data/raw/bus_trip_updates/YYYY-MM-DD/timestamp.pb
data/raw/bus_vehicle_positions/YYYY-MM-DD/timestamp.pb
data/raw/lrt_trip_updates/YYYY-MM-DD/timestamp.pb
data/raw/lrt_vehicle_positions/YYYY-MM-DD/timestamp.pb
data/static_gtfs/bus_static_gtfs/YYYY-MM-DD/GTFS.zip
data/static_gtfs/lrt_static_gtfs/YYYY-MM-DD/GTFS.zip
```

## Local Setup

```bash
python3 -m venv collector/.venv
collector/.venv/bin/python -m pip install -r collector/requirements.txt
cp .env.example .env
```

Edit `.env` if you want GCS uploads:

```bash
GCS_BUCKET=your-bucket-name
```

Run one collection batch:

```bash
collector/.venv/bin/python collector/collect_feeds.py --once
```

Run continuously:

```bash
collector/.venv/bin/python collector/collect_feeds.py
```

## GCP Deployment

The collector is currently designed to run on a small Compute Engine VM using `systemd`.

See:

- [GCP VM deployment](ops/gcp/README.md)