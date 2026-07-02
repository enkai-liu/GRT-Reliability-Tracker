"""Score live GTFS-RT feeds with the trained delay prediction model.

Each cycle:
  1. Fetches bus/LRT trip updates and vehicle positions directly from the GRT
     API into a rolling raw-snapshot history under data/live/raw (same layout
     as data/raw), pruning files older than the feature window.
  2. Fetches a fresh ECCC weather forecast when the cached one is stale.
  3. Rebuilds the training feature table over the window in DuckDB, mirroring
     build_delay_table.py + build_features.py (10-minute snapshot stride,
     lag features, vehicle-position join, weather join, static GTFS joins
     against the latest available static snapshot).
  4. Encodes features exactly as train_model.py does (including the
     hash % 100000 categorical encoding) and scores the newest snapshot.
  5. Writes dashboard/data/live-predictions.json for the frontend and appends
     the full scored rows to data/live/predictions_log for later evaluation
     against observed delays.

Lag features need history, so predictions are best after the scorer has been
running for an hour or more (or when pointed at a collector's raw directory).

Usage:
  collector/.venv/bin/python analysis/predict_live.py            # single cycle
  collector/.venv/bin/python analysis/predict_live.py --interval-seconds 300
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb
import lightgbm as lgb
import numpy as np
import pyarrow as pa

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "collector"))
sys.path.insert(0, str(PROJECT_ROOT / "analysis"))

from dotenv import load_dotenv  # noqa: E402

from collect_feeds import (  # noqa: E402
    configured_feeds,
    fetch_bytes,
    is_valid_realtime,
    make_gcs_bucket,
    make_session,
)
from parse_snapshots import TABLE_SCHEMAS, parse_snapshot  # noqa: E402
from build_weather_features import parse_forecast_file  # noqa: E402
from collect_weather_forecasts import fetch_forecast, DEFAULT_API_URL  # noqa: E402
from train_model import (  # noqa: E402
    BOOLEAN_FEATURES,
    CATEGORICAL_FEATURES,
    NUMERIC_FEATURES,
)

DEFAULT_LIVE_ROOT = PROJECT_ROOT / "data" / "live"
DEFAULT_STATIC_ROOT = PROJECT_ROOT / "data" / "parsed_static_gtfs"
DEFAULT_MODEL_ROOT = PROJECT_ROOT / "data" / "analysis" / "models_live"
DEFAULT_OUTPUT = PROJECT_ROOT / "dashboard" / "data" / "live-predictions.json"
DEFAULT_LOG_ROOT = DEFAULT_LIVE_ROOT / "predictions_log"

REALTIME_FEEDS = (
    "bus_trip_updates",
    "bus_vehicle_positions",
    "lrt_trip_updates",
    "lrt_vehicle_positions",
)
WEATHER_LOCATION = ("kitchener_waterloo", "on-82")
STATIC_TABLES = ("stop_times", "trips", "routes", "stops")
LATE_THRESHOLD_SECONDS = 300


def utc_now():
    return datetime.now(timezone.utc)


def timestamp_stem(now):
    return now.strftime("%Y-%m-%dT%H-%M-%SZ")


def fetch_realtime_snapshots(session, raw_root, now):
    """Fetch all realtime feeds and save them under the rolling history root."""
    feeds = configured_feeds()
    saved = []
    for feed_name in REALTIME_FEEDS:
        url = feeds[feed_name]["url"]
        try:
            content = fetch_bytes(session, url)
        except Exception as error:
            print(f"  WARN fetch {feed_name} failed: {error}")
            continue

        valid, detail = is_valid_realtime(content)
        if not valid:
            print(f"  WARN {feed_name} invalid: {detail}")
            continue

        target = raw_root / feed_name / now.strftime("%Y-%m-%d") / f"{timestamp_stem(now)}.pb"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content)
        saved.append(feed_name)
    return saved


def latest_weather_path(weather_root):
    location_dir = weather_root / WEATHER_LOCATION[0]
    if not location_dir.exists():
        return None
    paths = sorted(location_dir.glob("*/*.json"))
    return paths[-1] if paths else None


def refresh_weather(weather_root, now, max_age_minutes, api_url):
    latest = latest_weather_path(weather_root)
    if latest is not None:
        collected = datetime.strptime(latest.stem, "%Y-%m-%dT%H-%M-%SZ").replace(tzinfo=timezone.utc)
        if now - collected < timedelta(minutes=max_age_minutes):
            return latest

    location_name, location_id = WEATHER_LOCATION
    try:
        payload = fetch_forecast(location_id, api_url)
    except Exception as error:
        print(f"  WARN weather fetch failed: {error}")
        return latest

    target = weather_root / location_name / now.strftime("%Y-%m-%d") / f"{timestamp_stem(now)}.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return target


def prune_history(raw_root, now, window_minutes):
    """Delete snapshot files older than the feature window."""
    cutoff = now - timedelta(minutes=window_minutes)
    removed = 0
    for pattern in ("*/*/*.pb", "weather_forecasts/*/*/*.json"):
        for path in raw_root.glob(pattern):
            try:
                collected = datetime.strptime(path.stem, "%Y-%m-%dT%H-%M-%SZ").replace(tzinfo=timezone.utc)
            except ValueError:
                continue
            # Keep weather files longer: the latest forecast may be the only one.
            if path.suffix == ".json":
                if collected < now - timedelta(hours=12):
                    path.unlink()
                    removed += 1
            elif collected < cutoff:
                path.unlink()
                removed += 1
    return removed


def parse_window_snapshots(raw_root, now, window_minutes):
    """Parse all .pb files in the window into Arrow tables."""
    cutoff = now - timedelta(minutes=window_minutes)
    stop_time_rows = []
    vehicle_rows = []

    for feed_name in REALTIME_FEEDS:
        feed_dir = raw_root / feed_name
        if not feed_dir.exists():
            continue
        for path in sorted(feed_dir.glob("*/*.pb")):
            try:
                collected = datetime.strptime(path.stem, "%Y-%m-%dT%H-%M-%SZ").replace(tzinfo=timezone.utc)
            except ValueError:
                continue
            if collected < cutoff:
                continue
            rows = parse_snapshot(path, feed_name)
            stop_time_rows.extend(rows["stop_time_updates"])
            vehicle_rows.extend(rows["vehicle_positions"])

    stu = pa.Table.from_pylist(stop_time_rows, schema=TABLE_SCHEMAS["stop_time_updates"])
    vp = pa.Table.from_pylist(vehicle_rows, schema=TABLE_SCHEMAS["vehicle_positions"])
    return stu, vp


def parse_window_weather(weather_root):
    latest = latest_weather_path(weather_root)
    if latest is None:
        return None
    rows = parse_forecast_file(latest, WEATHER_LOCATION[0])
    if not rows:
        return None
    return pa.Table.from_pylist(rows)


def latest_static_dates(static_root):
    """Find the newest static snapshot date that has all required tables, per feed."""
    dates = {}
    for feed in ("bus_static_gtfs", "lrt_static_gtfs"):
        candidates = None
        for table in STATIC_TABLES:
            table_dir = static_root / table / f"p_feed={feed}"
            if not table_dir.exists():
                candidates = set()
                break
            found = {
                entry.name.removeprefix("p_snapshot_date=")
                for entry in table_dir.iterdir()
                if entry.is_dir() and (entry / "part-000.parquet").exists()
            }
            candidates = found if candidates is None else candidates & found
        if candidates:
            dates[feed] = max(candidates)
    return dates


def register_static_views(con, static_root, static_dates):
    for table in STATIC_TABLES:
        selects = []
        for feed, snapshot_date in static_dates.items():
            path = static_root / table / f"p_feed={feed}" / f"p_snapshot_date={snapshot_date}" / "part-000.parquet"
            selects.append(f"SELECT * FROM read_parquet('{path}')")
        con.sql(f"CREATE OR REPLACE VIEW static_{table} AS {' UNION ALL BY NAME '.join(selects)}")


def build_feature_query(stride_minutes, has_weather):
    """Feature SQL over the in-memory window tables.

    Mirrors build_delay_table.py + build_features.py with --snapshot-stride-minutes:
    keep the last observation per trip-stop per stride bucket, compute lag features
    over that strided history, then keep only the newest snapshot for scoring.
    """
    stride_seconds = stride_minutes * 60

    weather_join = ""
    weather_cols = """
            NULL::BIGINT AS temperature_c,
            NULL::BIGINT AS wind_speed_kmh,
            NULL::BIGINT AS wind_gust_kmh,
            NULL::BIGINT AS precip_probability_pct,
            NULL::VARCHAR AS precip_category,
            NULL::BOOLEAN AS is_rain,
            NULL::BOOLEAN AS is_snow,
            NULL::BOOLEAN AS is_precip,
    """
    if has_weather:
        weather_join = """
        LEFT JOIN weather_hourly w
            ON date_trunc('hour', d.scheduled_arrival_utc) = w.timestamp_utc_hour
        """
        weather_cols = """
            w.temperature_c,
            w.wind_speed_kmh,
            w.wind_gust_kmh,
            w.precip_probability_pct,
            w.precip_category,
            w.is_rain,
            w.is_snow,
            w.is_precip,
        """

    return f"""
    WITH realtime AS (
        SELECT
            feed_name,
            collected_at_utc,
            trip_id,
            route_id,
            direction_id,
            start_date,
            start_time,
            vehicle_id,
            stop_sequence,
            stop_id,
            arrival_time_utc,
            CASE
                WHEN feed_name LIKE 'bus_%' THEN 'bus_static_gtfs'
                WHEN feed_name LIKE 'lrt_%' THEN 'lrt_static_gtfs'
            END AS static_feed
        FROM stu
        WHERE arrival_time_utc IS NOT NULL
    ),

    joined AS (
        SELECT
            r.*,
            r.arrival_time_utc AS predicted_arrival_utc,
            strptime(r.start_date, '%Y%m%d')::DATE AS service_date,
            timezone('America/Toronto',
                ((strptime(r.start_date, '%Y%m%d')
                  - CASE
                      WHEN CAST(split_part(s.arrival_time, ':', 1) AS INTEGER) >= 24
                       AND CAST(split_part(r.start_time, ':', 1) AS INTEGER) < 6
                      THEN INTERVAL 1 DAY
                      ELSE INTERVAL 0 DAY
                    END
                 )::TIMESTAMP
                 + INTERVAL (CAST(split_part(s.arrival_time, ':', 1) AS INTEGER)) HOUR
                 + INTERVAL (CAST(split_part(s.arrival_time, ':', 2) AS INTEGER)) MINUTE
                 + INTERVAL (CAST(split_part(s.arrival_time, ':', 3) AS INTEGER)) SECOND
                )
            ) AS scheduled_arrival_utc
        FROM realtime r
        JOIN static_stop_times s
            ON r.trip_id = s.trip_id
            AND r.stop_id = s.stop_id
            AND CASE
                    WHEN r.static_feed = 'lrt_static_gtfs' THEN r.stop_sequence - 1
                    ELSE r.stop_sequence
                END = CAST(s.stop_sequence AS INTEGER)
            AND r.static_feed = s.p_feed
    ),

    delay_source_all AS (
        SELECT
            *,
            epoch(predicted_arrival_utc) - epoch(scheduled_arrival_utc) AS delay_seconds,
            (epoch(predicted_arrival_utc) - epoch(collected_at_utc)) / 60.0 AS prediction_lead_minutes
        FROM joined
        WHERE scheduled_arrival_utc IS NOT NULL
          AND (epoch(predicted_arrival_utc) - epoch(collected_at_utc)) >= 0
    ),

    delay_source AS (
        SELECT * EXCLUDE (rn)
        FROM (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY trip_id, stop_id,
                                 floor(epoch(collected_at_utc) / {stride_seconds})
                    ORDER BY collected_at_utc DESC
                ) AS rn
            FROM delay_source_all
        )
        WHERE rn = 1
    ),

    delay_lag_features AS (
        SELECT
            d.*,
            d.delay_seconds AS current_predicted_delay_seconds,
            ROW_NUMBER() OVER trip_stop_history - 1 AS prior_prediction_count,
            lag(d.delay_seconds) OVER trip_stop_history
                AS previous_predicted_delay_seconds,
            lag(d.delay_seconds, 2) OVER trip_stop_history
                AS previous_2_predicted_delay_seconds,
            lag(d.delay_seconds, 5) OVER trip_stop_history
                AS previous_5_predicted_delay_seconds,
            d.delay_seconds - lag(d.delay_seconds) OVER trip_stop_history
                AS predicted_delay_delta_seconds,
            (epoch(d.collected_at_utc) - epoch(lag(d.collected_at_utc) OVER trip_stop_history)) / 60.0
                AS minutes_since_previous_prediction,
            avg(d.delay_seconds) OVER recent_trip_stop_history
                AS recent_predicted_delay_mean_5,
            min(d.delay_seconds) OVER recent_trip_stop_history
                AS recent_predicted_delay_min_5,
            max(d.delay_seconds) OVER recent_trip_stop_history
                AS recent_predicted_delay_max_5,
            lag(d.delay_seconds) OVER trip_snapshot_stops
                AS previous_stop_predicted_delay_seconds,
            d.delay_seconds - lag(d.delay_seconds) OVER trip_snapshot_stops
                AS previous_stop_predicted_delay_delta_seconds
        FROM delay_source d
        WINDOW
            trip_stop_history AS (
                PARTITION BY d.trip_id, d.stop_id
                ORDER BY d.collected_at_utc
            ),
            recent_trip_stop_history AS (
                PARTITION BY d.trip_id, d.stop_id
                ORDER BY d.collected_at_utc
                ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
            ),
            trip_snapshot_stops AS (
                PARTITION BY d.trip_id, d.collected_at_utc
                ORDER BY d.stop_sequence
            )
    ),

    vehicle_positions AS (
        SELECT
            collected_at_utc,
            trip_id,
            any_value(latitude) AS latitude,
            any_value(longitude) AS longitude,
            any_value(speed) AS speed,
            any_value(current_stop_sequence) AS current_stop_sequence,
            any_value(current_status) AS current_status,
            max(vehicle_timestamp_utc) AS vehicle_timestamp_utc
        FROM vp
        WHERE trip_id IS NOT NULL
        GROUP BY collected_at_utc, trip_id
    ),

    trip_max_seq AS (
        SELECT trip_id, p_feed, MAX(CAST(stop_sequence AS INTEGER)) AS max_stop_seq
        FROM static_stop_times
        GROUP BY trip_id, p_feed
    ),

    routes AS (
        SELECT DISTINCT p_feed, route_id, route_short_name, route_type
        FROM static_routes
    ),

    trips AS (
        SELECT DISTINCT p_feed, trip_id, trip_headsign, direction_id AS static_direction_id
        FROM static_trips
    ),

    stops AS (
        SELECT DISTINCT p_feed, stop_id, stop_name, stop_lat, stop_lon
        FROM static_stops
    )

    SELECT
        d.trip_id,
        d.route_id,
        d.stop_id,
        d.stop_sequence,
        COALESCE(d.direction_id, TRY_CAST(t.static_direction_id AS BIGINT)) AS direction_id,
        d.vehicle_id,
        d.feed_name,
        d.service_date,

        d.current_predicted_delay_seconds,
        d.collected_at_utc,
        d.scheduled_arrival_utc,
        d.predicted_arrival_utc,
        d.prediction_lead_minutes,
        extract(hour FROM d.scheduled_arrival_utc AT TIME ZONE 'America/Toronto')::INTEGER
            AS hour_of_day,
        extract(minute FROM d.scheduled_arrival_utc AT TIME ZONE 'America/Toronto')::INTEGER
            AS minute_of_hour,
        extract(dow FROM d.scheduled_arrival_utc AT TIME ZONE 'America/Toronto')::INTEGER
            AS day_of_week,
        extract(dow FROM d.scheduled_arrival_utc AT TIME ZONE 'America/Toronto')::INTEGER IN (0, 6)
            AS is_weekend,
        (extract(hour FROM d.scheduled_arrival_utc AT TIME ZONE 'America/Toronto')::INTEGER
            BETWEEN 7 AND 8)
        OR (extract(hour FROM d.scheduled_arrival_utc AT TIME ZONE 'America/Toronto')::INTEGER
            BETWEEN 16 AND 17)
            AS is_rush_hour,

        r.route_short_name,
        CAST(r.route_type AS INTEGER) AS route_type,
        t.trip_headsign,
        s.stop_name,
        CAST(s.stop_lat AS DOUBLE) AS stop_lat,
        CAST(s.stop_lon AS DOUBLE) AS stop_lon,

        CASE WHEN tms.max_stop_seq > 0
            THEN (
                CASE
                    WHEN d.feed_name LIKE 'lrt_%' THEN d.stop_sequence - 1
                    ELSE d.stop_sequence
                END
            )::DOUBLE / tms.max_stop_seq
            ELSE 0.0
        END AS stop_sequence_normalized,

        d.prior_prediction_count,
        d.previous_predicted_delay_seconds,
        d.previous_2_predicted_delay_seconds,
        d.previous_5_predicted_delay_seconds,
        d.predicted_delay_delta_seconds,
        d.minutes_since_previous_prediction,
        d.recent_predicted_delay_mean_5,
        d.recent_predicted_delay_min_5,
        d.recent_predicted_delay_max_5,
        d.previous_stop_predicted_delay_seconds,
        d.previous_stop_predicted_delay_delta_seconds,

        vp.latitude AS vehicle_lat,
        vp.longitude AS vehicle_lon,
        vp.speed AS vehicle_speed,
        vp.current_stop_sequence AS vehicle_current_stop_sequence,
        vp.current_status AS vehicle_current_status,
        CASE
            WHEN vp.vehicle_timestamp_utc IS NOT NULL
            THEN epoch(d.collected_at_utc) - epoch(vp.vehicle_timestamp_utc)
        END AS vehicle_update_age_seconds,
        CASE
            WHEN vp.vehicle_timestamp_utc IS NOT NULL
            THEN epoch(d.collected_at_utc) - epoch(vp.vehicle_timestamp_utc) > 120
        END AS is_vehicle_update_stale,
        CASE
            WHEN vp.latitude IS NOT NULL AND vp.longitude IS NOT NULL
             AND TRY_CAST(s.stop_lat AS DOUBLE) IS NOT NULL
             AND TRY_CAST(s.stop_lon AS DOUBLE) IS NOT NULL
            THEN 6371000 * 2 * asin(sqrt(
                pow(sin(radians((TRY_CAST(s.stop_lat AS DOUBLE) - vp.latitude) / 2)), 2)
                + cos(radians(vp.latitude)) * cos(radians(TRY_CAST(s.stop_lat AS DOUBLE)))
                * pow(sin(radians((TRY_CAST(s.stop_lon AS DOUBLE) - vp.longitude) / 2)), 2)
            ))
        END AS vehicle_distance_to_stop_m,
        CASE
            WHEN vp.current_stop_sequence IS NOT NULL
            THEN (
                CASE
                    WHEN d.feed_name LIKE 'lrt_%' THEN d.stop_sequence - 1
                    ELSE d.stop_sequence
                END
            ) - vp.current_stop_sequence
        END AS vehicle_stop_sequence_delta,

        {weather_cols}

        CASE WHEN d.feed_name LIKE 'bus_%' THEN 'bus' ELSE 'lrt' END AS transit_mode

    FROM delay_lag_features d
    LEFT JOIN routes r
        ON d.route_id = r.route_id AND d.static_feed = r.p_feed
    LEFT JOIN trips t
        ON d.trip_id = t.trip_id AND d.static_feed = t.p_feed
    LEFT JOIN stops s
        ON d.stop_id = s.stop_id AND d.static_feed = s.p_feed
    LEFT JOIN trip_max_seq tms
        ON d.trip_id = tms.trip_id AND d.static_feed = tms.p_feed
    LEFT JOIN vehicle_positions vp
        ON d.collected_at_utc = vp.collected_at_utc AND d.trip_id = vp.trip_id
    {weather_join}
    WHERE d.collected_at_utc = (SELECT max(collected_at_utc) FROM delay_source_all)
    ORDER BY d.trip_id, d.stop_sequence
    """


def encode_and_predict(con, model, quantile_models):
    """Encode live_features exactly as train_model.py and score all models.

    Returns (point_predictions, lower_predictions, upper_predictions); the
    interval arrays are None when no quantile models are available.
    """
    feature_select = []
    for f in NUMERIC_FEATURES:
        feature_select.append(f"COALESCE({f}, 0)::DOUBLE AS {f}")
    for f in BOOLEAN_FEATURES:
        feature_select.append(f"CAST(COALESCE({f}, false) AS DOUBLE) AS {f}")
    for f in CATEGORICAL_FEATURES:
        feature_select.append(
            f"CAST(hash(COALESCE(CAST({f} AS VARCHAR), '')) % 100000 AS INTEGER) AS {f}"
        )

    rows = con.sql(f"""
        SELECT {", ".join(feature_select)}
        FROM live_features
        ORDER BY trip_id, stop_sequence
    """).fetchall()

    if not rows:
        return np.array([]), None, None

    X = np.array(rows, dtype=np.float64)
    predictions = model.predict(X)

    if len(quantile_models) < 2:
        return predictions, None, None

    quantile_preds = {q: m.predict(X) for q, m in quantile_models.items()}
    lo_q, hi_q = min(quantile_preds), max(quantile_preds)
    # Quantile models are trained independently, so enforce lower <= point <= upper.
    lower = np.minimum(quantile_preds[lo_q], predictions)
    upper = np.maximum(quantile_preds[hi_q], predictions)
    return predictions, lower, upper


def summarize(meta_rows, predictions, now, model_path, lower=None, upper=None,
              interval_quantiles=None, vehicles=None):
    """Build the dashboard JSON payload.

    ``vehicles`` is the full live marker list (one per moving vehicle, every
    route) built by ``build_live_vehicles``; pass ``None`` for no markers. It is
    kept independent of model scoring so selecting any route on the map shows
    all of its buses, not just the trips the model happened to score.
    """
    routes = {}
    arrivals = []
    trips = set()

    has_interval = lower is not None and upper is not None

    for i, (row, predicted) in enumerate(zip(meta_rows, predictions)):
        mode = row["transit_mode"]
        key = f"{mode}:{row['route_id']}"
        trips.add(row["trip_id"])

        entry = routes.setdefault(key, {
            "key": key,
            "transit_mode": mode,
            "route_id": row["route_id"],
            "route_short_name": row["route_short_name"] or row["route_id"],
            "arrivals": 0,
            "trips": set(),
            "sum_predicted": 0.0,
            "max_predicted": float("-inf"),
            "sum_feed": 0.0,
            "late_arrivals": 0,
        })
        entry["arrivals"] += 1
        entry["trips"].add(row["trip_id"])
        entry["sum_predicted"] += predicted
        entry["max_predicted"] = max(entry["max_predicted"], predicted)
        entry["sum_feed"] += row["current_predicted_delay_seconds"]
        if predicted > LATE_THRESHOLD_SECONDS:
            entry["late_arrivals"] += 1

        arrival_entry = {
            "routeKey": key,
            "route_short_name": row["route_short_name"] or row["route_id"],
            "transit_mode": mode,
            "trip_headsign": row["trip_headsign"],
            "stop_id": row["stop_id"],
            "stop_name": row["stop_name"],
            "direction_id": row["direction_id"],
            "scheduled_arrival_utc": row["scheduled_arrival_utc"].isoformat(),
            "eta_minutes": round(row["prediction_lead_minutes"], 1),
            "predicted_delay_seconds": round(float(predicted)),
            "feed_delay_seconds": round(row["current_predicted_delay_seconds"]),
        }
        if has_interval:
            arrival_entry["predicted_delay_lower_seconds"] = round(float(lower[i]))
            arrival_entry["predicted_delay_upper_seconds"] = round(float(upper[i]))
        arrivals.append(arrival_entry)

    route_list = []
    for entry in sorted(routes.values(), key=lambda e: -(e["sum_predicted"] / e["arrivals"])):
        route_list.append({
            "key": entry["key"],
            "transit_mode": entry["transit_mode"],
            "route_id": entry["route_id"],
            "route_short_name": entry["route_short_name"],
            "arrivals": entry["arrivals"],
            "trips": len(entry["trips"]),
            "mean_predicted_delay_seconds": round(entry["sum_predicted"] / entry["arrivals"]),
            "max_predicted_delay_seconds": round(entry["max_predicted"]),
            "mean_feed_delay_seconds": round(entry["sum_feed"] / entry["arrivals"]),
            "late_arrivals": entry["late_arrivals"],
        })

    arrivals.sort(key=lambda a: -a["predicted_delay_seconds"])

    vehicle_list = vehicles or []

    return {
        "generatedAtUtc": now.isoformat(),
        "model": str(model_path),
        "lateThresholdSeconds": LATE_THRESHOLD_SECONDS,
        "intervalQuantiles": list(interval_quantiles) if has_interval and interval_quantiles else None,
        "totals": {
            "stopArrivals": len(arrivals),
            "trips": len(trips),
            "routes": len(route_list),
        },
        "routes": route_list,
        "vehicles": vehicle_list,
        "worstArrivals": arrivals[:40],
    }


def build_trip_predictions(meta_rows, predictions, lower=None, upper=None):
    """Per-trip prediction enrichment for the map markers.

    Returns ``{trip_id: {next_stop_name, next_stop_eta_minutes,
    predicted_delay_seconds, ...}}`` keyed on each trip's *next* upcoming scored
    arrival (smallest lead time). Trips the model did not score are simply
    absent, and their markers render without a prediction.
    """
    has_interval = lower is not None and upper is not None
    best = {}
    for i, (row, predicted) in enumerate(zip(meta_rows, predictions)):
        trip_id = row["trip_id"]
        lead = row["prediction_lead_minutes"]
        current = best.get(trip_id)
        if current is not None and lead >= current["_lead"]:
            continue
        entry = {
            "_lead": lead,
            "next_stop_name": row["stop_name"],
            "next_stop_eta_minutes": round(lead, 1),
            "predicted_delay_seconds": round(float(predicted)),
        }
        if has_interval:
            entry["predicted_delay_lower_seconds"] = round(float(lower[i]))
            entry["predicted_delay_upper_seconds"] = round(float(upper[i]))
        best[trip_id] = entry
    for entry in best.values():
        entry.pop("_lead", None)
    return best


# Latest reported position per vehicle, joined to static GTFS for route/headsign.
# Independent of model scoring so every moving bus on a route shows up — the
# feed gives route_id + trip_id on every vehicle; bearing/direction are usually
# null there, so direction comes from the static trips table.
LIVE_VEHICLE_QUERY = """
WITH latest_vp AS (
    SELECT * EXCLUDE (rn) FROM (
        SELECT
            *,
            CASE WHEN feed_name LIKE 'lrt_%' THEN 'lrt_static_gtfs' ELSE 'bus_static_gtfs' END AS static_feed,
            CASE WHEN feed_name LIKE 'bus_%' THEN 'bus' ELSE 'lrt' END AS transit_mode,
            row_number() OVER (
                PARTITION BY coalesce(vehicle_id, entity_id)
                ORDER BY vehicle_timestamp_utc DESC NULLS LAST, collected_at_utc DESC
            ) AS rn
        FROM vp
        WHERE latitude IS NOT NULL AND longitude IS NOT NULL
          AND route_id IS NOT NULL AND route_id <> ''
    ) WHERE rn = 1
),
route_lookup AS (SELECT DISTINCT p_feed, route_id, route_short_name FROM static_routes),
trip_lookup AS (SELECT DISTINCT p_feed, trip_id, trip_headsign, direction_id FROM static_trips)
SELECT
    v.transit_mode,
    v.route_id,
    coalesce(r.route_short_name, v.route_id) AS route_short_name,
    v.trip_id,
    coalesce(v.direction_id, TRY_CAST(t.direction_id AS BIGINT)) AS direction_id,
    t.trip_headsign,
    v.latitude AS lat,
    v.longitude AS lon,
    v.vehicle_timestamp_utc
FROM latest_vp v
LEFT JOIN route_lookup r ON v.route_id = r.route_id AND v.static_feed = r.p_feed
LEFT JOIN trip_lookup t ON v.trip_id = t.trip_id AND v.static_feed = t.p_feed
ORDER BY v.transit_mode, v.route_id
"""


def build_live_vehicles(con, now, trip_predictions):
    """Build the full live marker list from the raw vehicle-position snapshot,
    enriching each with its trip's prediction (next stop, predicted delay) where
    the model scored it."""
    relation = con.sql(LIVE_VEHICLE_QUERY)
    cols = relation.columns
    markers = []
    for raw in relation.fetchall():
        row = dict(zip(cols, raw))
        direction = row["direction_id"]
        marker = {
            "routeKey": f"{row['transit_mode']}:{row['route_id']}",
            "transit_mode": row["transit_mode"],
            "route_short_name": row["route_short_name"] or row["route_id"],
            "trip_headsign": row["trip_headsign"],
            "direction_id": int(direction) if direction is not None else None,
            "lat": round(float(row["lat"]), 6),
            "lon": round(float(row["lon"]), 6),
        }
        timestamp = row["vehicle_timestamp_utc"]
        if timestamp is not None:
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=timezone.utc)
            marker["position_age_seconds"] = max(0, round((now - timestamp).total_seconds()))
        prediction = trip_predictions.get(row["trip_id"])
        if prediction:
            marker.update(prediction)
        markers.append(marker)
    markers.sort(key=lambda m: (m["routeKey"], m["trip_headsign"] or ""))
    return markers


def write_predictions_log(con, predictions, log_root, now, lower=None, upper=None):
    con.sql("CREATE OR REPLACE TABLE log_meta AS SELECT * FROM live_features ORDER BY trip_id, stop_sequence")
    has_interval = lower is not None and upper is not None
    pred_columns = {
        "rn": pa.array(range(1, len(predictions) + 1), type=pa.int64()),
        "predicted_delay_seconds": pa.array(predictions, type=pa.float64()),
        "predicted_delay_lower_seconds": pa.array(
            lower if has_interval else [None] * len(predictions), type=pa.float64()),
        "predicted_delay_upper_seconds": pa.array(
            upper if has_interval else [None] * len(predictions), type=pa.float64()),
    }
    pred_table = pa.table(pred_columns)
    con.register("pred_table", pred_table)
    log_dir = log_root / f"date={now.strftime('%Y-%m-%d')}"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"run-{timestamp_stem(now)}.parquet"
    con.sql(f"""
        COPY (
            SELECT
                m.collected_at_utc, m.feed_name, m.transit_mode, m.trip_id, m.route_id,
                m.stop_id, m.stop_sequence, m.direction_id, m.service_date,
                m.scheduled_arrival_utc, m.predicted_arrival_utc, m.prediction_lead_minutes,
                m.current_predicted_delay_seconds,
                p.predicted_delay_seconds,
                p.predicted_delay_lower_seconds,
                p.predicted_delay_upper_seconds
            FROM (SELECT *, ROW_NUMBER() OVER (ORDER BY trip_id, stop_sequence) AS rn FROM log_meta) m
            JOIN pred_table p USING (rn)
        ) TO '{log_path}' (FORMAT PARQUET, COMPRESSION SNAPPY)
    """)
    return log_path


def load_quantile_models(model_root):
    """Load lgbm_model_qNN.txt boosters keyed by quantile, e.g. {0.1: ..., 0.9: ...}."""
    models = {}
    for path in sorted(model_root.glob("lgbm_model_q*.txt")):
        try:
            quantile = int(path.stem.removeprefix("lgbm_model_q")) / 100
        except ValueError:
            continue
        models[quantile] = lgb.Booster(model_file=str(path))
    return models


def write_output(payload, output_path, bucket=None, object_name=None):
    """Write the dashboard live JSON locally, and — when a GCS bucket is given —
    upload it as a no-cache object so a statically hosted dashboard (e.g. GitHub
    Pages) can fetch fresh predictions cross-origin each minute."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")
    if bucket is not None and object_name:
        blob = bucket.blob(object_name)
        blob.cache_control = "no-cache, max-age=0"
        blob.upload_from_filename(str(output_path), content_type="application/json")


def upload_predictions_log(log_bucket, log_path):
    """Mirror a per-run predictions-log parquet to the private GCS bucket so
    evaluation can run anywhere without pulling files off the scorer host."""
    object_name = f"live/predictions_log/{log_path.parent.name}/{log_path.name}"
    try:
        log_bucket.blob(object_name).upload_from_filename(str(log_path))
    except Exception as exc:
        print(f"  WARN predictions-log upload failed ({object_name}): {exc}")


def run_cycle(args, session, model, quantile_models, bucket=None, log_bucket=None):
    now = utc_now()
    raw_root = args.live_root / "raw"
    weather_root = raw_root / "weather_forecasts"

    print(f"[{timestamp_stem(now)}] fetching feeds...")
    saved = fetch_realtime_snapshots(session, raw_root, now)
    if not any(name.endswith("trip_updates") for name in saved):
        print("  no trip update feeds fetched, skipping cycle")
        return

    refresh_weather(weather_root, now, args.weather_max_age_minutes, args.weather_api_url)
    prune_history(raw_root, now, args.window_minutes + args.stride_minutes)

    stu, vp = parse_window_snapshots(raw_root, now, args.window_minutes)
    print(f"  window: {stu.num_rows} stop time updates, {vp.num_rows} vehicle positions")
    if stu.num_rows == 0:
        print("  nothing to score")
        return

    static_dates = latest_static_dates(args.static_root)
    if not static_dates:
        raise SystemExit(f"No parsed static GTFS found under {args.static_root}")

    con = duckdb.connect()
    con.sql("SET timezone = 'UTC'")
    con.register("stu", stu)
    con.register("vp", vp)
    register_static_views(con, args.static_root, static_dates)

    weather = parse_window_weather(weather_root)
    has_weather = weather is not None
    if has_weather:
        con.register("weather_raw", weather)
        con.sql("""
            CREATE OR REPLACE VIEW weather_hourly AS
            SELECT * EXCLUDE (rn)
            FROM (
                SELECT *,
                    date_trunc('hour', forecast_timestamp_utc::TIMESTAMPTZ) AS timestamp_utc_hour,
                    ROW_NUMBER() OVER (
                        PARTITION BY date_trunc('hour', forecast_timestamp_utc::TIMESTAMPTZ)
                        ORDER BY collected_at_filename DESC
                    ) AS rn
                FROM weather_raw
            )
            WHERE rn = 1
        """)
    else:
        print("  WARN no weather forecast available; weather features will be null")

    query = build_feature_query(args.stride_minutes, has_weather)
    con.sql(f"CREATE OR REPLACE TABLE live_features AS {query}")

    feature_count = con.sql("SELECT count(*) FROM live_features").fetchone()[0]
    if feature_count == 0:
        print("  no scoreable arrivals in the latest snapshot (static GTFS join may be stale)")
        # still publish live positions, just without per-trip predictions
        vehicles = build_live_vehicles(con, now, {})
        payload = summarize([], np.array([]), now, args.model_root / "lgbm_model.txt", vehicles=vehicles)
        write_output(payload, args.output, bucket, args.gcs_object)
        con.close()
        return

    predictions, lower, upper = encode_and_predict(con, model, quantile_models)

    meta_relation = con.sql("SELECT * FROM live_features ORDER BY trip_id, stop_sequence")
    meta_cols = meta_relation.columns
    meta_rows = [dict(zip(meta_cols, row)) for row in meta_relation.fetchall()]

    trip_predictions = build_trip_predictions(meta_rows, predictions, lower=lower, upper=upper)
    vehicles = build_live_vehicles(con, now, trip_predictions)

    interval_quantiles = sorted(quantile_models) if len(quantile_models) >= 2 else None
    payload = summarize(
        meta_rows, predictions, now, args.model_root / "lgbm_model.txt",
        lower=lower, upper=upper, interval_quantiles=interval_quantiles, vehicles=vehicles,
    )
    write_output(payload, args.output, bucket, args.gcs_object)

    log_path = write_predictions_log(con, predictions, args.log_root, now, lower=lower, upper=upper)
    if log_bucket is not None:
        upload_predictions_log(log_bucket, log_path)
    con.close()

    mean_pred = float(np.mean(predictions))
    print(
        f"  scored {feature_count} arrivals across {payload['totals']['trips']} trips "
        f"({payload['totals']['routes']} routes), mean predicted delay {mean_pred:.0f}s; "
        f"{len(vehicles)} live vehicles on the map"
    )
    print(f"  wrote {args.output} and {log_path}")


def parse_args():
    parser = argparse.ArgumentParser(description="Score live GTFS-RT feeds with the trained delay model.")
    parser.add_argument("--live-root", type=Path, default=DEFAULT_LIVE_ROOT,
                        help="Root for the rolling raw snapshot history and logs.")
    parser.add_argument("--static-root", type=Path, default=DEFAULT_STATIC_ROOT,
                        help="Root of parsed static GTFS Parquet tables.")
    parser.add_argument("--model-root", type=Path, default=DEFAULT_MODEL_ROOT,
                        help="Directory containing lgbm_model.txt.")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT,
                        help="Dashboard JSON output path.")
    parser.add_argument("--log-root", type=Path, default=DEFAULT_LOG_ROOT,
                        help="Directory for per-run scored Parquet logs.")
    parser.add_argument("--window-minutes", type=int, default=90,
                        help="Snapshot history window for lag features.")
    parser.add_argument("--stride-minutes", type=int, default=10,
                        help="Snapshot stride bucket; must match the trained model's feature build.")
    parser.add_argument("--weather-max-age-minutes", type=int, default=30,
                        help="Re-fetch the ECCC forecast when the cached one is older than this.")
    parser.add_argument("--weather-api-url", default=DEFAULT_API_URL)
    parser.add_argument("--interval-seconds", type=int,
                        help="Run continuously, scoring every N seconds. Defaults to a single cycle.")
    parser.add_argument("--gcs-bucket", default=os.getenv("GCS_LIVE_BUCKET"),
                        help="Public GCS bucket to upload live-predictions.json to so a static "
                             "dashboard can fetch it. Defaults to the GCS_LIVE_BUCKET env var; "
                             "no upload when unset.")
    parser.add_argument("--gcs-object", default="live/live-predictions.json",
                        help="Object name for the uploaded live JSON within --gcs-bucket.")
    parser.add_argument("--log-gcs-bucket", default=os.getenv("GCS_BUCKET"),
                        help="Private GCS bucket to mirror per-run predictions-log parquet files "
                             "to (live/predictions_log/date=*/run-*.parquet). Defaults to the "
                             "GCS_BUCKET env var; no upload when unset.")
    return parser.parse_args()


def main():
    load_dotenv(PROJECT_ROOT / ".env")
    args = parse_args()
    args.live_root = args.live_root.resolve()
    args.static_root = args.static_root.resolve()
    args.model_root = args.model_root.resolve()
    args.output = args.output.resolve()
    args.log_root = args.log_root.resolve()

    model_path = args.model_root / "lgbm_model.txt"
    if not model_path.exists():
        raise SystemExit(f"Model not found: {model_path}. Train one with analysis/train_model.py first.")
    model = lgb.Booster(model_file=str(model_path))

    quantile_models = load_quantile_models(args.model_root)
    if len(quantile_models) >= 2:
        labels = ", ".join(f"q{q:g}" for q in sorted(quantile_models))
        print(f"Loaded quantile models for prediction intervals: {labels}")
    else:
        quantile_models = {}
        print("No quantile models found; predictions will not include intervals. "
              "Retrain with analysis/train_model.py to generate them.")

    session = make_session()

    bucket = make_gcs_bucket(args.gcs_bucket) if args.gcs_bucket else None
    if bucket is not None:
        print(f"Uploading live predictions to: gs://{bucket.name}/{args.gcs_object}")

    log_bucket = make_gcs_bucket(args.log_gcs_bucket) if args.log_gcs_bucket else None
    if log_bucket is not None:
        print(f"Mirroring predictions log to: gs://{log_bucket.name}/live/predictions_log/")

    if not args.interval_seconds:
        run_cycle(args, session, model, quantile_models, bucket, log_bucket)
        return

    print(f"Scoring every {args.interval_seconds}s. Ctrl-C to stop.")
    while True:
        started = time.monotonic()
        try:
            run_cycle(args, session, model, quantile_models, bucket, log_bucket)
        except KeyboardInterrupt:
            raise
        except Exception as error:
            print(f"  ERROR cycle failed: {error}")
        elapsed = time.monotonic() - started
        time.sleep(max(1.0, args.interval_seconds - elapsed))


if __name__ == "__main__":
    main()
