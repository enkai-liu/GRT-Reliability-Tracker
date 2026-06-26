"""Export reliability dashboard JSON for the static frontend."""

import argparse
import json
from datetime import date
from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RELIABILITY_ROOT = PROJECT_ROOT / "data" / "analysis" / "reliability"
DEFAULT_TRANSFERS_ROOT = PROJECT_ROOT / "data" / "analysis" / "transfers"
DEFAULT_STATIC_ROOT = PROJECT_ROOT / "data" / "parsed_static_gtfs"
DEFAULT_OUTPUT = PROJECT_ROOT / "dashboard" / "data" / "dashboard-data.json"
DEFAULT_MODEL_EVAL = PROJECT_ROOT / "data" / "analysis" / "models_live" / "evaluation.json"


def iso_date(value):
    return date.fromisoformat(value).isoformat()


def build_model_payload(model_eval_path):
    """Live-model held-out accuracy, so the dashboard can back its "predicted"
    claims. Returns None when the evaluation file isn't present."""
    path = Path(model_eval_path)
    if not path.exists():
        return None
    data = json.loads(path.read_text())
    lgbm = data.get("lgbm", {})
    baselines = data.get("baselines", {})
    val_dates = data.get("val_dates", [])
    val_mae = lgbm.get("val_mae")
    if val_mae is None:
        return None
    rnd = lambda v: round(v, 1) if isinstance(v, (int, float)) else None
    return {
        "valMaeSeconds": rnd(val_mae),
        "rawFeedMaeSeconds": rnd(baselines.get("raw GTFS-RT prediction")),
        "scheduleMaeSeconds": rnd(baselines.get("schedule (delay=0)")),
        "valStart": val_dates[0] if val_dates else None,
        "valEnd": val_dates[-1] if val_dates else None,
        "valDays": len(val_dates),
    }


def route_key(mode, route_id):
    return f"{mode}:{route_id}"


def fetch_dicts(con, query):
    relation = con.sql(query)
    cols = relation.columns
    return [dict(zip(cols, row)) for row in relation.fetchall()]


def build_transfers_payload(con, transfers_root):
    """Transfer reliability summaries, when build_transfer_reliability.py has run."""
    route_pairs_path = transfers_root / "transfer_route_pairs.parquet"
    if not route_pairs_path.exists():
        return {"byRoute": {}, "worst": []}

    items = fetch_dicts(con, f"""
        SELECT
            from_mode, from_route_id, to_mode, to_route_id, transfer_stop_name,
            attempts, success_rate,
            median_scheduled_wait_seconds, median_margin_seconds, days_observed
        FROM read_parquet('{route_pairs_path}')
        ORDER BY attempts DESC
    """)

    by_route = {}
    for item in items:
        item["fromKey"] = route_key(item["from_mode"], item["from_route_id"])
        item["toKey"] = route_key(item["to_mode"], item["to_route_id"])
        by_route.setdefault(item["fromKey"], []).append(item)

    # Keep the busiest connections per route so the payload stays small.
    for key, rows in by_route.items():
        rows.sort(key=lambda r: r["attempts"], reverse=True)
        by_route[key] = rows[:20]

    worst = sorted(
        (item for item in items if item["attempts"] >= 100),
        key=lambda r: r["success_rate"],
    )[:20]

    return {"byRoute": by_route, "worst": worst}


def build_dashboard_payload(reliability_root, transfers_root, static_root, start_date=None,
                            model_eval_path=DEFAULT_MODEL_EVAL):
    con = duckdb.connect()
    con.sql("SET timezone = 'UTC'")

    # Note: this only trims the daily series and overall stats. The route,
    # hour, and stop summaries are pre-aggregated; rebuild them with
    # build_reliability_tables.py --start-date for a fully filtered export.
    # snapshot_date is stored as an ISO string, so string comparison is safe
    date_filter = f"WHERE snapshot_date >= '{start_date}'" if start_date else ""

    system_by_date = fetch_dicts(con, f"""
        SELECT *
        FROM read_parquet('{reliability_root}/system_by_date.parquet')
        {date_filter}
        ORDER BY snapshot_date
    """)

    route_items = fetch_dicts(con, f"""
        SELECT *
        FROM read_parquet('{reliability_root}/route_summary.parquet')
        ORDER BY transit_mode, route_short_name
    """)
    routes = []
    for item in route_items:
        item["key"] = route_key(item["transit_mode"], item["route_id"])
        routes.append(item)

    route_hour_items = fetch_dicts(con, f"""
        SELECT *
        FROM read_parquet('{reliability_root}/route_by_hour.parquet')
        ORDER BY transit_mode, route_id, hour_of_day
    """)
    route_by_hour = {}
    for item in route_hour_items:
        route_by_hour.setdefault(route_key(item["transit_mode"], item["route_id"]), []).append(item)

    stop_items = fetch_dicts(con, f"""
        SELECT *
        FROM read_parquet('{reliability_root}/route_stop_summary.parquet')
        WHERE stop_lat IS NOT NULL
          AND stop_lon IS NOT NULL
        ORDER BY transit_mode, route_id, direction_id, stop_name
    """)
    route_stops = {}
    for item in stop_items:
        key = route_key(item["transit_mode"], item["route_id"])
        route_stops.setdefault(key, []).append(item)

    latest_static_date = con.sql(f"""
        SELECT max(p_snapshot_date)
        FROM read_parquet('{static_root}/shapes/p_feed=*/p_snapshot_date=*/part-000.parquet',
                          hive_partitioning=true)
    """).fetchone()[0]

    shape_rows = con.sql(f"""
        WITH route_shapes AS (
            SELECT DISTINCT
                CASE WHEN t.p_feed = 'bus_static_gtfs' THEN 'bus' ELSE 'lrt' END AS transit_mode,
                t.route_id,
                t.shape_id,
                t.p_feed,
                t.p_snapshot_date
            FROM read_parquet('{static_root}/trips/p_feed=*/p_snapshot_date=*/part-000.parquet',
                              hive_partitioning=true) t
            WHERE t.p_snapshot_date = DATE '{latest_static_date}'
        )
        SELECT
            rs.transit_mode,
            rs.route_id,
            rs.shape_id,
            CAST(s.shape_pt_lat AS DOUBLE) AS lat,
            CAST(s.shape_pt_lon AS DOUBLE) AS lon,
            CAST(s.shape_pt_sequence AS INTEGER) AS seq
        FROM route_shapes rs
        JOIN read_parquet('{static_root}/shapes/p_feed=*/p_snapshot_date=*/part-000.parquet',
                          hive_partitioning=true) s
            ON rs.shape_id = s.shape_id
            AND rs.p_feed = s.p_feed
            AND rs.p_snapshot_date = s.p_snapshot_date
        WHERE s.shape_pt_lat IS NOT NULL
          AND s.shape_pt_lon IS NOT NULL
        ORDER BY rs.transit_mode, rs.route_id, rs.shape_id, seq
    """).fetchall()

    route_shapes = {}
    bounds = {"minLat": 999.0, "maxLat": -999.0, "minLon": 999.0, "maxLon": -999.0}
    for mode, route_id, shape_id, lat, lon, _seq in shape_rows:
        key = route_key(mode, route_id)
        route_shapes.setdefault(key, {}).setdefault(shape_id, []).append([round(lat, 6), round(lon, 6)])
        bounds["minLat"] = min(bounds["minLat"], lat)
        bounds["maxLat"] = max(bounds["maxLat"], lat)
        bounds["minLon"] = min(bounds["minLon"], lon)
        bounds["maxLon"] = max(bounds["maxLon"], lon)

    route_shape_list = {
        key: [{"shapeId": shape_id, "points": points} for shape_id, points in shapes.items()]
        for key, shapes in route_shapes.items()
    }

    overall = con.sql(f"""
        SELECT
            sum(observations) AS observations,
            min(snapshot_date) AS start_date,
            max(snapshot_date) AS end_date,
            avg(on_time_rate) AS avg_daily_on_time_rate,
            median(median_delay_seconds) AS median_daily_delay_seconds,
            quantile_cont(p90_delay_seconds, 0.90) AS p90_daily_p90_delay_seconds
        FROM read_parquet('{reliability_root}/system_by_date.parquet')
        {date_filter}
    """).fetchone()

    return {
        "generatedFrom": {
            "reliabilityRoot": str(reliability_root),
            "staticSnapshotDate": str(latest_static_date),
        },
        "overall": {
            "observations": overall[0],
            "startDate": overall[1],
            "endDate": overall[2],
            "avgDailyOnTimeRate": overall[3],
            "medianDailyDelaySeconds": overall[4],
            "p90DailyP90DelaySeconds": overall[5],
        },
        "bounds": bounds,
        "systemByDate": system_by_date,
        "routes": routes,
        "routeByHour": route_by_hour,
        "routeStops": route_stops,
        "routeShapes": route_shape_list,
        "transfers": build_transfers_payload(con, transfers_root),
        "model": build_model_payload(model_eval_path),
    }


def parse_args():
    parser = argparse.ArgumentParser(description="Export JSON data for the static dashboard.")
    parser.add_argument("--reliability-root", type=Path, default=DEFAULT_RELIABILITY_ROOT)
    parser.add_argument("--transfers-root", type=Path, default=DEFAULT_TRANSFERS_ROOT)
    parser.add_argument("--static-root", type=Path, default=DEFAULT_STATIC_ROOT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument(
        "--start-date",
        type=iso_date,
        help="Exclude daily-series rows before this date (YYYY-MM-DD). Route-level "
        "tables are pre-aggregated; rebuild them with build_reliability_tables.py "
        "--start-date for a fully filtered export.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    payload = build_dashboard_payload(
        args.reliability_root.resolve(),
        args.transfers_root.resolve(),
        args.static_root.resolve(),
        args.start_date,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")
    print(f"Wrote {args.output} ({len(payload['routes'])} routes)")


if __name__ == "__main__":
    main()
