# Reliability Dashboard

Static frontend for exploring GRT reliability summaries.

## Build Data

Generate reliability tables first, then export dashboard JSON:

```bash
collector/.venv/bin/python analysis/build_reliability_tables.py
collector/.venv/bin/python analysis/export_dashboard_data.py
```

The exporter writes `dashboard/data/dashboard-data.json`. That file is generated
from local analysis data and is intentionally ignored by Git.

## Run Locally

```bash
collector/.venv/bin/python -m http.server 8765 --directory dashboard
```

Open `http://127.0.0.1:8765/`.

## UI

- Route reliability map built from latest static GTFS shapes
- Route search, mode filter, and sorting
- Click routes from the map or list
- Mouse wheel or trackpad to zoom
- Drag to pan
- Use `-`, home, and `+` buttons to zoom out, reset, and zoom in
