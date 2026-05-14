#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="/opt/grt-reliability-tracker"
PYTHON="$PROJECT_DIR/collector/.venv/bin/python"
PARSE_DATE="${1:-$(date -u -d yesterday +%F)}"

cd "$PROJECT_DIR"

echo "Parsing GTFS-Realtime snapshots for $PARSE_DATE"
"$PYTHON" "$PROJECT_DIR/collector/parse_snapshots.py" \
  --date "$PARSE_DATE" \
  --sync-from-gcs \
  --upload-to-gcs \
  --overwrite

echo "Parsing static GTFS snapshots for $PARSE_DATE"
"$PYTHON" "$PROJECT_DIR/collector/parse_static_gtfs.py" \
  --date "$PARSE_DATE" \
  --sync-from-gcs \
  --upload-to-gcs \
  --overwrite

echo "Daily parse complete for $PARSE_DATE"
