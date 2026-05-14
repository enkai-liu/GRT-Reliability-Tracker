# GRT Reliability Tracker

Collector and parsing tools for Grand River Transit GTFS-Realtime and static GTFS data, including vehicle positions, trip updates, service alerts, and static schedules.

## Setup

```bash
python3 -m venv collector/.venv
collector/.venv/bin/python -m pip install -r collector/requirements.txt
cp .env.example .env
```

Set `GCS_BUCKET` in `.env` to upload snapshots and parsed tables to Google Cloud Storage.

## Collect

```bash
collector/.venv/bin/python collector/collect_feeds.py --once
collector/.venv/bin/python collector/collect_feeds.py
```

## Parse

```bash
collector/.venv/bin/python collector/parse_snapshots.py --date YYYY-MM-DD --sync-from-gcs --upload-to-gcs --overwrite
collector/.venv/bin/python collector/parse_static_gtfs.py --date YYYY-MM-DD --sync-from-gcs --upload-to-gcs --overwrite
```

## Health Check

```bash
collector/.venv/bin/python collector/health_check.py
```

## Deployment

See [ops/gcp/README.md](ops/gcp/README.md) for the Compute Engine VM deployment.
