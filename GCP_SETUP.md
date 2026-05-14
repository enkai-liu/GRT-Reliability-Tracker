# GCP Setup

This project can write local GTFS snapshots and optionally upload each snapshot to Google Cloud Storage.

## 1. Install and Sign In

Install the Google Cloud CLI, then run:

```bash
gcloud init
gcloud auth application-default login
```

The second command sets up Application Default Credentials for local Python code.

## 2. Create a Project

Use an existing project or create a new one:

```bash
gcloud projects create grt-reliability-tracker
gcloud config set project grt-reliability-tracker
```

Enable billing for the project in the Google Cloud Console.

## 3. Create a Storage Bucket

Bucket names are globally unique, so change the name if this one is taken:

```bash
gcloud storage buckets create gs://grt-reliability-raw-data \
  --location=us-east1 \
  --uniform-bucket-level-access
```

Recommended bucket settings:

```text
Location type: Region
Region: us-east1 or us-central1
Storage class: Standard
Public access prevention: On
Access control: Uniform
Object versioning: Disabled
Retention: Disabled
Encryption: Google-managed
```

## 4. Configure the Collector

Copy the example environment file:

```bash
cp .env.example .env
```

Edit `.env` and set:

```bash
GCS_BUCKET=grt-reliability-raw-data
```

## 5. Install Dependencies

Create a local virtual environment if needed:

```bash
python3 -m venv collector/.venv
```

Install dependencies:

```bash
collector/.venv/bin/python -m pip install -r collector/requirements.txt
```

## 6. Test One Collection Batch

```bash
collector/.venv/bin/python collector/collect_feeds.py --once
```

You should see local paths if `GCS_BUCKET` is empty, or `gs://...` paths if uploads are enabled.

## 7. Run Continuously

```bash
collector/.venv/bin/python collector/collect_feeds.py
```

The collector polls realtime feeds every 30 seconds and saves static GTFS ZIPs once per UTC day.

## Production VM

For always-on collection, deploy to Compute Engine:

```bash
ops/gcp/deploy_to_vm.sh
```

See [ops/gcp/README.md](ops/gcp/README.md).

## Useful Commands

List uploaded files:

```bash
gcloud storage ls --recursive gs://grt-reliability-raw-data
```

Download a day for local analysis:

```bash
gcloud storage cp --recursive \
  gs://grt-reliability-raw-data/raw/bus_trip_updates/2026-05-13 \
  data/raw/bus_trip_updates/
```

Sync all raw data locally:

```bash
gcloud storage rsync --recursive gs://grt-reliability-raw-data/raw data/raw
```
