# GRT Reliability Tracker

Collector and parsing tools for Grand River Transit GTFS-Realtime and static GTFS data, including vehicle positions, trip updates, service alerts, and static schedules.

By default, trip updates and vehicle positions are collected every 30 seconds. Service alerts are collected every 5 minutes.
Kitchener-Waterloo and Cambridge weather forecast snapshots are collected every 3 hours on the GCP VM.

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

## Reliability Analysis

```bash
collector/.venv/bin/python analysis/build_delay_table.py --overwrite
collector/.venv/bin/python analysis/build_weather_features.py --overwrite
collector/.venv/bin/python analysis/build_features.py --overwrite
collector/.venv/bin/python analysis/build_reliability_tables.py
collector/.venv/bin/python analysis/export_dashboard_data.py
```

See [analysis/README.md](analysis/README.md) for the generated summary tables.

## Delay Prediction Training

```bash
collector/.venv/bin/python analysis/build_delay_table.py --overwrite \
  --keep-all-snapshots --output-root data/analysis/delay_table_snapshots
collector/.venv/bin/python analysis/build_features.py --overwrite \
  --delay-root data/analysis/delay_table_snapshots \
  --output-root data/analysis/features_live \
  --snapshot-stride-minutes 10
collector/.venv/bin/python analysis/train_model.py \
  --features-root data/analysis/features_live \
  --output-root data/analysis/models_live \
  --max-train-rows 2000000 \
  --max-val-rows 500000 \
  --late-delay-weight 3.0
```

## Dashboard

```bash
collector/.venv/bin/python -m http.server 8765 --directory dashboard
```

Open `http://127.0.0.1:8765/`. The dashboard reads generated local JSON from
`dashboard/data/dashboard-data.json`, which is ignored by Git.

## Deployment

See [ops/gcp/README.md](ops/gcp/README.md) for the Compute Engine VM deployment.
