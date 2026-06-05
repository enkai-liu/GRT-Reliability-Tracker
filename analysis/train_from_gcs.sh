#!/usr/bin/env bash
# Train delay prediction model using all data from GCS.
#
# Usage:
#   ./analysis/train_from_gcs.sh                    # all available dates
#   ./analysis/train_from_gcs.sh 2026-05-14 2026-06-04  # specific date range
#
# Prerequisites:
#   - gcloud auth application-default login
#   - collector/.venv with all dependencies installed

set -euo pipefail
cd "$(dirname "$0")/.."

PYTHON=collector/.venv/bin/python
BUCKET=grt-reliability-raw-data

if [ $# -ge 2 ]; then
    START_DATE="$1"
    END_DATE="$2"
else
    # Discover available dates from GCS
    echo "Discovering available dates on GCS..."
    START_DATE=$($PYTHON -c "
from google.cloud import storage
client = storage.Client(project='grt-reliability-raw-data')
bucket = client.bucket('$BUCKET')
dates = set()
blobs_iter = bucket.list_blobs(prefix='raw/bus_trip_updates/', delimiter='/')
list(blobs_iter)  # consume iterator
for prefix in blobs_iter.prefixes:
    dates.add(prefix.rstrip('/').split('/')[-1])
dates = sorted(dates)
print(dates[0])
")
    END_DATE=$($PYTHON -c "
from google.cloud import storage
client = storage.Client(project='grt-reliability-raw-data')
bucket = client.bucket('$BUCKET')
dates = set()
blobs_iter = bucket.list_blobs(prefix='raw/bus_trip_updates/', delimiter='/')
list(blobs_iter)
for prefix in blobs_iter.prefixes:
    dates.add(prefix.rstrip('/').split('/')[-1])
dates = sorted(dates)
print(dates[-1])
")
fi

echo "Date range: $START_DATE to $END_DATE"

# Generate list of dates
DATES=$($PYTHON -c "
from datetime import date, timedelta
start = date.fromisoformat('$START_DATE')
end = date.fromisoformat('$END_DATE')
d = start
while d <= end:
    print(d.isoformat())
    d += timedelta(days=1)
")

# Build --date flags
DATE_FLAGS=""
for d in $DATES; do
    DATE_FLAGS="$DATE_FLAGS --date $d"
done

echo ""
echo "=== Step 1/6: Sync raw snapshots from GCS ==="
$PYTHON collector/parse_snapshots.py \
    --sync-from-gcs --gcs-bucket "$BUCKET" \
    $DATE_FLAGS --overwrite

echo ""
echo "=== Step 2/6: Sync static GTFS from GCS ==="
$PYTHON collector/parse_static_gtfs.py \
    --sync-from-gcs --gcs-bucket "$BUCKET" \
    $DATE_FLAGS --overwrite

echo ""
echo "=== Step 3/6: Build delay table ==="
$PYTHON analysis/build_delay_table.py $DATE_FLAGS --overwrite

echo ""
echo "=== Step 4/6: Build weather features ==="
$PYTHON analysis/build_weather_features.py --overwrite

echo ""
echo "=== Step 5/6: Build enriched features ==="
$PYTHON analysis/build_features.py $DATE_FLAGS --overwrite

echo ""
echo "=== Step 6/6: Train model ==="
$PYTHON analysis/train_model.py

echo ""
echo "=== Done ==="
echo "Model artifacts: data/analysis/models/"
echo "Evaluation:      data/analysis/models/evaluation.txt"
