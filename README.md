# GRT Reliability Tracker

Collector and parsing tools for Grand River Transit GTFS-Realtime and static GTFS data, including vehicle positions, trip updates, service alerts, and static schedules.

By default, trip updates and vehicle positions are collected every 30 seconds. Service alerts are collected every 5 minutes.
Weather forecast snapshots are collected every 3 hours on the GCP VM.

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
collector/.venv/bin/python collector/collect_weather_forecasts.py
```

## Parse

```bash
collector/.venv/bin/python collector/parse_snapshots.py --date YYYY-MM-DD --sync-from-gcs --upload-to-gcs --overwrite
collector/.venv/bin/python collector/parse_static_gtfs.py --date YYYY-MM-DD --sync-from-gcs --upload-to-gcs --overwrite
collector/.venv/bin/python collector/run_local_parse.py --start-date YYYY-MM-DD --end-date YYYY-MM-DD
```

## Health Check

```bash
collector/.venv/bin/python collector/health_check.py
```

## Deployment

See [ops/gcp/README.md](ops/gcp/README.md) for the Compute Engine VM deployment.
