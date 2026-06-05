# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

GRT Reliability Tracker collects and parses Grand River Transit GTFS-Realtime feeds, static GTFS archives, and ECCC weather forecasts. Data is stored locally or in Google Cloud Storage, with raw protobuf/JSON snapshots parsed into Parquet tables for analysis.

## Setup & Common Commands

```bash
# Setup
python3 -m venv collector/.venv
collector/.venv/bin/python -m pip install -r collector/requirements.txt
cp .env.example .env

# Collection
collector/.venv/bin/python collector/collect_feeds.py --once       # single cycle
collector/.venv/bin/python collector/collect_feeds.py              # continuous (30s poll)
collector/.venv/bin/python collector/collect_weather_forecasts.py

# Parsing
collector/.venv/bin/python collector/parse_snapshots.py --date YYYY-MM-DD
collector/.venv/bin/python collector/parse_static_gtfs.py --date YYYY-MM-DD
collector/.venv/bin/python collector/run_local_parse.py            # parses yesterday by default

# Debugging
collector/.venv/bin/python collector/inspect_pb.py <file.pb|file.zip>

# Health check (GCS)
collector/.venv/bin/python collector/health_check.py --bucket <BUCKET>
```

No test suite or linter is configured.

## Architecture

**Data pipeline with separated collection and parsing stages:**

1. **collect_feeds.py** — Polls GRT API every 30s for realtime protobuf feeds (bus + LRT trip updates, vehicle positions) and every 5min for service alerts. Downloads static GTFS ZIPs once daily. Optionally uploads to GCS.

2. **collect_weather_forecasts.py** — Fetches ECCC weather forecasts for configured locations (Kitchener-Waterloo, Cambridge). Runs on 3-hour timer on GCP VM.

3. **parse_snapshots.py** — Batch converts raw `.pb` snapshots into 7 Parquet tables: `feed_snapshots`, `vehicle_positions`, `trip_updates`, `stop_time_updates`, `service_alerts`, `service_alert_active_periods`, `service_alert_informed_entities`. Explicit PyArrow schemas define each table.

4. **parse_static_gtfs.py** — Extracts CSV files from GTFS ZIP archives into Parquet, partitioned by feed and snapshot date. Schema is inferred from CSV headers.

5. **health_check.py** — Monitors GCS data freshness with configurable staleness thresholds per feed type.

6. **run_local_parse.py** — Convenience orchestrator that runs both parsers for a date range.

**Data storage layout:**
- Raw: `data/raw/{feed_name}/{date}/TIMESTAMP.pb`
- Static GTFS: `data/static_gtfs/{feed}/{date}/GTFS.zip`
- Weather: `data/raw/weather_forecasts/{location}/{date}/TIMESTAMP.json`
- Parsed realtime: `data/parsed/{table}/date={date}/part-000.parquet`
- Parsed static: `data/parsed_static_gtfs/{table}/p_feed={feed}/p_snapshot_date={date}/part-000.parquet`

## Key Design Decisions

- **Separate collection from parsing**: Collection runs 24/7 on a small GCP VM (e2-micro); parsing can run locally on demand.
- **Two poll rates**: Realtime feeds at 30s, alerts at 5min — configured via `POLL_SECONDS` / `ALERT_POLL_SECONDS`.
- **Optional GCS backend**: All collectors work locally without GCS; production uses GCS for durability.
- **Legacy TLS**: GRT API requires `SECLEVEL=1` for older server compatibility.

## Deployment

- **GCP VM**: `ops/gcp/deploy_to_vm.sh` provisions a Compute Engine VM with systemd services for collection, daily parsing, and weather fetching.
- **macOS**: `ops/launchd/` contains LaunchAgent plists for local background collection and daily parsing at 05:30.
