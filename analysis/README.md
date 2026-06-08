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
