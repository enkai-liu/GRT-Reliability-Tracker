# Reliability Dashboard

Static frontend for exploring GRT reliability summaries — "The Reliability
Atlas". Self-contained in `index.html` (no separate CSS/JS).

Other pages kept for reference:

- `legacy.html` — the previous dashboard (uses `styles.css` + `app.js`)
- `redesign.html` — the A/B design prototype the current site grew out of

## Build Data

Generate reliability tables first, then export dashboard JSON:

```bash
collector/.venv/bin/python analysis/build_reliability_tables.py
collector/.venv/bin/python analysis/export_dashboard_data.py
collector/.venv/bin/python analysis/export_timetable.py   # trip planner timetable (defaults to today)
collector/.venv/bin/python analysis/build_basemap.py      # geographic basemap (one-off; see Basemap)
```

Both scripts (and `analysis/build_transfer_reliability.py`) accept
`--start-date YYYY-MM-DD` to exclude earlier days — used to drop the partial
first collection day (May 13, 2026), e.g.:

```bash
collector/.venv/bin/python analysis/build_reliability_tables.py --start-date 2026-05-14
collector/.venv/bin/python analysis/build_transfer_reliability.py --summaries-only --start-date 2026-05-14
collector/.venv/bin/python analysis/export_dashboard_data.py --start-date 2026-05-14
```

The exporter writes `dashboard/data/dashboard-data.json`. That file is generated
from local analysis data and is intentionally ignored by Git.

## Run Locally

```bash
collector/.venv/bin/python -m http.server 8765 --directory dashboard
```

Open `http://127.0.0.1:8765/`.

## UI

Three views under one top bar (choice persists in `localStorage`):

**Atlas** — the map instrument:

- Full-bleed network map from latest static GTFS shapes, colored by on-time
  rate; every route is clickable (wide hit areas) and keyboard-focusable,
  with a thicken-and-glow hover highlight
- Mouse wheel / trackpad to zoom, drag to pan, `-` / `⌖` / `+` buttons
- Route rail with system stats, search, and mode filter
- Clicking a route (map or rail) opens a detail card: metrics, live
  predicted delays, on-time by hour, riskiest connections, and the full
  stop list; `Esc` or `✕` closes it
- Selecting a route shows the live positions of its vehicles on the map
  (refreshed every minute, colored by predicted delay; shown only while
  the live scorer's data is fresh)

**Planner** — the reliability-aware trip planner:

- Pure RAPTOR router (`planner.js`, no DOM dependencies — portable to a
  server runtime unchanged) over `data/timetable.json`, exported per service
  day by `analysis/export_timetable.py`
- Origin/destination by clicking the map (A then B) or stop-name search;
  departure time defaults to now
- Itineraries ranked by predicted p90 arrival plus a missed-connection
  penalty — not just scheduled time; each transfer shows its observed
  make-rate (or an estimate from slack vs. the incoming route's delays)
- Fresh live predictions shift near-term legs and flag them in the results
- The chosen itinerary is drawn on the map with walk legs dashed

**Field Report** — the editorial analysis page:

- Headline on-time stat and system summary
- "Field notes" — written insights whose figures auto-fill from the data
  (`data-fact` spans); edit the commentary placeholders in `index.html`
  (search for `FIELD-NOTES`)
- Daily on-time trend chart with weekend shading and hover tooltips
- Live conditions strip with the hardest-hit upcoming arrivals
- League table of every route (filter, sort, click to expand a drill-down)
- Riskiest transfer connections system-wide

## Live Predictions

The live panel reads `data/live-predictions.json`, written by the live scorer:

```bash
collector/.venv/bin/python analysis/predict_live.py --interval-seconds 300
```

The panel hides itself when the file is missing and shows a "stale" badge when
the last scoring run is more than 10 minutes old.

## Basemap

The Atlas/Planner map draws a faint geographic basemap (water, the Grand &
Speed rivers, major roads, and the Region of Waterloo boundary) *under* the
colored routes, using the same projection so everything aligns. It reads
`data/basemap.json`, built once from OpenStreetMap:

```bash
collector/.venv/bin/python analysis/build_basemap.py
```

The script caches raw Overpass responses under `data/basemap_cache/` (gitignored)
so re-runs and filter tweaks don't re-hit the network. The map falls back to the
plain survey grid if `basemap.json` is missing. The projection is
cosine-latitude corrected and fits the network to the visible map area (the gap
beside the rail / planner panel), so the network keeps true proportions.

## Transfers

Transfer success rates come from `analysis/build_transfer_reliability.py`;
run it before `export_dashboard_data.py` to include them in the dashboard
JSON. The section hides itself when no transfer data is present.
