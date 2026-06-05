"""Parse ECCC weather forecast JSON snapshots into an hourly weather feature table.

Reads raw JSON forecasts from data/raw/weather_forecasts/{location}/{date}/*.json,
extracts hourly weather fields, and outputs a Parquet table keyed by
(timestamp_utc_hour, location) for joining with delay data.

When multiple forecast snapshots cover the same hour, the latest snapshot
(closest to the forecast hour) is kept.

Output: data/analysis/weather_features/part-000.parquet
"""

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pyarrow as pa

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RAW_ROOT = PROJECT_ROOT / "data" / "raw" / "weather_forecasts"
DEFAULT_OUTPUT_PATH = PROJECT_ROOT / "data" / "analysis" / "weather_features" / "part-000.parquet"

# ECCC icon codes for precipitation categories
RAIN_ICON_CODES = {12, 13, 14, 28}
SNOW_ICON_CODES = {16, 17, 18, 25, 26, 27}
FREEZING_RAIN_ICON_CODES = {7, 8, 19}


def safe_get(obj, *keys, default=None):
    for key in keys:
        if isinstance(obj, dict):
            obj = obj.get(key)
        else:
            return default
        if obj is None:
            return default
    return obj


def parse_forecast_file(file_path, location_name):
    data = json.loads(file_path.read_text(encoding="utf-8"))
    collected_at = file_path.stem  # e.g. "2026-05-14T13-00-00Z"

    props = data.get("properties") or {}
    hourly_group = props.get("hourlyForecastGroup") or {}
    hourly_forecasts = hourly_group.get("hourlyForecasts") or []

    rows = []
    for h in hourly_forecasts:
        timestamp_str = h.get("timestamp")
        if not timestamp_str:
            continue

        icon_code = safe_get(h, "iconCode", "value")
        icon_code_int = int(icon_code) if icon_code is not None else None

        rows.append({
            "location": location_name,
            "forecast_timestamp_utc": timestamp_str,
            "collected_at_filename": collected_at,
            "temperature_c": safe_get(h, "temperature", "value", "en"),
            "condition": safe_get(h, "condition", "en"),
            "icon_code": icon_code_int,
            "wind_speed_kmh": safe_get(h, "wind", "speed", "value", "en"),
            "wind_gust_kmh": safe_get(h, "wind", "gust", "value", "en"),
            "wind_direction": safe_get(h, "wind", "direction", "value", "en"),
            "precip_probability_pct": safe_get(h, "lop", "value", "en"),
            "precip_category": safe_get(h, "lop", "category", "en"),
            "uv_index": safe_get(h, "uv", "index", "value", "en"),
            "is_rain": icon_code_int in RAIN_ICON_CODES if icon_code_int is not None else None,
            "is_snow": icon_code_int in SNOW_ICON_CODES if icon_code_int is not None else None,
            "is_freezing_rain": icon_code_int in FREEZING_RAIN_ICON_CODES if icon_code_int is not None else None,
            "is_precip": icon_code_int in (RAIN_ICON_CODES | SNOW_ICON_CODES | FREEZING_RAIN_ICON_CODES)
                if icon_code_int is not None else None,
        })

    return rows


def collect_all_rows(raw_root, dates):
    all_rows = []
    selected_dates = set(dates) if dates else None

    for location_dir in sorted(raw_root.iterdir()):
        if not location_dir.is_dir():
            continue
        location_name = location_dir.name

        for date_dir in sorted(location_dir.iterdir()):
            if not date_dir.is_dir():
                continue
            if selected_dates and date_dir.name not in selected_dates:
                continue

            for json_path in sorted(date_dir.glob("*.json")):
                rows = parse_forecast_file(json_path, location_name)
                all_rows.extend(rows)

    return all_rows


def deduplicate_hourly(con):
    """Keep the latest forecast snapshot for each (location, hour)."""
    return con.sql("""
        WITH parsed AS (
            SELECT *,
                -- Truncate to hour for joining with delay data
                date_trunc('hour', forecast_timestamp_utc::TIMESTAMPTZ) AS timestamp_utc_hour
            FROM weather_raw
        ),
        ranked AS (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY location, timestamp_utc_hour
                    ORDER BY collected_at_filename DESC
                ) AS rn
            FROM parsed
        )
        SELECT * EXCLUDE (rn, collected_at_filename, forecast_timestamp_utc)
        FROM ranked
        WHERE rn = 1
        ORDER BY location, timestamp_utc_hour
    """)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Build hourly weather feature table from ECCC forecast snapshots."
    )
    parser.add_argument(
        "--raw-root", type=Path, default=DEFAULT_RAW_ROOT,
        help="Root directory of raw weather forecast JSON files.",
    )
    parser.add_argument(
        "--output", type=Path, default=DEFAULT_OUTPUT_PATH,
        help="Output Parquet file path.",
    )
    parser.add_argument(
        "--date", action="append",
        help="Date to process in YYYY-MM-DD form. May be repeated. Defaults to all.",
    )
    parser.add_argument("--overwrite", action="store_true", help="Replace existing output.")
    return parser.parse_args()


def main():
    args = parse_args()
    raw_root = args.raw_root.resolve()
    output_path = args.output.resolve()

    if output_path.exists() and not args.overwrite:
        raise FileExistsError(f"{output_path} already exists. Pass --overwrite to replace it.")

    print(f"Scanning weather forecasts in {raw_root}...")
    all_rows = collect_all_rows(raw_root, args.date)

    if not all_rows:
        print("No weather forecast data found.")
        raise SystemExit(1)

    print(f"Parsed {len(all_rows)} hourly forecast entries")

    con = duckdb.connect()
    con.sql("SET timezone = 'UTC'")
    arrow_table = pa.Table.from_pylist(all_rows)
    con.sql("CREATE TABLE weather_raw AS SELECT * FROM arrow_table")

    result = deduplicate_hourly(con)
    final_count = con.sql("SELECT count(*) FROM result").fetchone()[0]
    print(f"Deduplicated to {final_count} unique (location, hour) entries")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    con.sql(f"COPY result TO '{output_path}' (FORMAT PARQUET, COMPRESSION SNAPPY)")
    print(f"Wrote {output_path}")


if __name__ == "__main__":
    main()
