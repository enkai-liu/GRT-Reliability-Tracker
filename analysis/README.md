# Analysis Workflows

Build the intermediate feature table first, then generate reliability summaries:

```bash
collector/.venv/bin/python analysis/build_delay_table.py --overwrite
collector/.venv/bin/python analysis/build_weather_features.py --overwrite
collector/.venv/bin/python analysis/build_features.py --overwrite
collector/.venv/bin/python analysis/build_reliability_tables.py
collector/.venv/bin/python analysis/export_dashboard_data.py
```

The reliability table builder reads `data/analysis/features` and writes:

- `data/analysis/reliability/system_by_date.csv`
- `data/analysis/reliability/route_summary.csv`
- `data/analysis/reliability/route_by_hour.csv`
- `data/analysis/reliability/route_by_day_of_week.csv`
- `data/analysis/reliability/stop_summary.csv`
- `data/analysis/reliability/route_stop_summary.csv`
- `data/analysis/reliability/dashboard_report.md`

Parquet versions are written alongside the CSV files.

The dashboard exporter reads those reliability tables plus latest static GTFS
shapes and writes `dashboard/data/dashboard-data.json`. That JSON is generated
locally and ignored by Git.

Default reliability definitions:

- Early: more than 60 seconds early
- On-time: between 60 seconds early and 300 seconds late
- Late: more than 300 seconds late

Useful options:

```bash
collector/.venv/bin/python analysis/build_reliability_tables.py \
  --date 2026-06-01 \
  --date 2026-06-02 \
  --min-observations 500
```

## Delay Prediction

For training, keep every GTFS-RT stop-time snapshot, then build a live feature
table with a 10-minute stride so the dataset remains practical for local model
training:

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

The live feature table uses the final pre-arrival delay for each trip-stop as
the target, preserves the current GTFS-RT predicted delay as an input, and adds
snapshot-history and vehicle-position features.

`--late-delay-weight` increases the training/evaluation weight for rows where
the final delay is above `--late-delay-threshold-seconds`, which defaults to
300 seconds.

`--quantiles` (default `0.1,0.9`) additionally trains one quantile-objective
LightGBM model per quantile, saved as `lgbm_model_q10.txt` / `lgbm_model_q90.txt`
next to the point model. These power prediction intervals ("arrives +2 to
+9 min"); `evaluation.txt` reports pinball loss per quantile plus the empirical
coverage and width of the interval. Pass an empty string to skip them.

### Live Scoring

`predict_live.py` serves the trained model against the current GTFS-RT feeds:

```bash
collector/.venv/bin/python analysis/predict_live.py                        # one cycle
collector/.venv/bin/python analysis/predict_live.py --interval-seconds 300 # continuous
```

Each cycle fetches trip updates and vehicle positions directly from the GRT
API into a rolling history under `data/live/raw`, rebuilds the training
features over a 90-minute window (10-minute snapshot stride, matching the
trained model), scores the newest snapshot, and writes:

- `dashboard/data/live-predictions.json` — per-route summaries and the worst
  upcoming arrivals, loaded by the dashboard's live panel.
- `data/live/predictions_log/date=YYYY-MM-DD/run-TIMESTAMP.parquet` — full
  scored rows for later evaluation against observed delays. When `GCS_BUCKET`
  (or `--log-gcs-bucket`) is set, each run file is also mirrored to
  `gs://<bucket>/live/predictions_log/` so evaluation can run on any machine.

When `lgbm_model_qNN.txt` quantile models are present in `--model-root`, each
arrival also gets `predicted_delay_lower_seconds` / `predicted_delay_upper_seconds`
(clamped so lower ≤ point ≤ upper), and the dashboard shows the range
("+2 to +9 min") instead of a single delay value.

Notes:

- Snapshot-history lag features start out null on a cold start; predictions
  are most faithful to training once the scorer has been running for an hour.
- `--stride-minutes` must match the `--snapshot-stride-minutes` used to build
  the training features.
- Static GTFS joins use the latest snapshot under `data/parsed_static_gtfs`.
  If GRT has published a new schedule since, refresh it first:
  `collector/.venv/bin/python collector/parse_static_gtfs.py --sync-from-gcs --gcs-bucket grt-reliability-raw-data --date <today>`.
- Categorical features are encoded with DuckDB's `hash(...) % 100000`, exactly
  as in `train_model.py`; both scripts must run with the same DuckDB version.

## Bus Bunching

`build_bunching.py` detects bunching from observed headways in the feature
table (so run `build_features.py` first):

```bash
collector/.venv/bin/python analysis/build_bunching.py
```

For each route/direction/stop, trips are ordered by scheduled arrival and the
observed headway behind the preceding vehicle is compared to the scheduled
headway. An arrival is **bunched** when the observed headway is below 0.5x the
scheduled headway (`--bunched-ratio`) and **gapped** above 1.5x
(`--gapped-ratio`). Only scheduled headways between 2 and 30 minutes are
considered. Summaries include excess wait time: the extra average wait a
randomly arriving rider experiences versus even service, from headway
variability (E[h²]/2E[h], observed minus scheduled).

Outputs under `data/analysis/bunching/`:

- `events/date=YYYY-MM-DD/part-000.parquet` — one row per observed headway
- `system_by_date`, `route_summary`, `route_by_hour`, `stop_summary` (CSV + Parquet)
- `bunching_report.md`

## Transfer Reliability

`build_transfer_reliability.py` measures how often connections between routes
actually work:

```bash
collector/.venv/bin/python analysis/build_transfer_reliability.py --overwrite
collector/.venv/bin/python analysis/export_dashboard_data.py   # include in dashboard
```

For each observed arrival it proposes the connection a trip planner would: the
first scheduled departure of every other route within walking distance
(default 150 m, walk time at 1.2 m/s with a 30 s floor) departing within
`--max-wait-minutes` (default 30). The transfer is "made" when that specific
vehicle actually departed at or after the rider's actual arrival plus walk
time, using the final pre-arrival GTFS-RT observations.

Outputs under `data/analysis/transfers/`:

- `events/date=YYYY-MM-DD/part-000.parquet` — one row per proposed connection
- `transfer_stop_summary.parquet/.csv` — per route-pair and stop-pair
- `transfer_route_pairs.parquet/.csv` — per route-pair at a named location

Caveats: scheduled departures are approximated by scheduled arrivals (nearly
always identical in GRT's schedule), and "actual" times are the feed's final
pre-arrival predictions rather than ground-truth door closings.
