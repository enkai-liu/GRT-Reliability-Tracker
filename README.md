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

- [GCP setup](GCP_SETUP.md)
- [GCP VM deployment](ops/gcp/README.md)

## macOS Background Mode

For local development, the collector can also run as a macOS LaunchAgent.

See:

- [LaunchAgent setup](ops/launchd/README.md)

## Manual Inspection

Use `collector/inspect_pb.py` to verify downloaded `.pb` files:

```bash
collector/.venv/bin/python collector/inspect_pb.py path/to/file.pb
```

## Repository Hygiene

The repo intentionally ignores:

- `.env`
- virtual environments
- logs
- raw `.pb` and static GTFS data under `data/`
- credential JSON files

Keep raw data in GCS or local ignored folders, not in Git.
