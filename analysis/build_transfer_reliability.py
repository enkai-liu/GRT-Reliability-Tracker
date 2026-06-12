"""Compute historical transfer (connection) reliability between routes.

For each observed arrival, finds the connection a trip planner would propose:
the first scheduled departure of every other route reachable within walking
distance (default 150 m), departing after the arrival plus walk time, within
the maximum wait window (default 30 min). The transfer succeeds when that
specific vehicle actually departed at or after the rider's actual arrival plus
walk time — both taken from the final pre-arrival GTFS-RT observations in the
delay table.

Scheduled departure times are approximated by the scheduled arrival time at
the stop, which is almost always identical in GRT's schedule. Actual departure
falls back to actual arrival when the feed omitted a departure time.

Outputs (under data/analysis/transfers/):
  - events/date=YYYY-MM-DD/part-000.parquet  — one row per proposed connection
  - transfer_stop_summary.parquet/.csv       — per route-pair at a stop pair
  - transfer_route_pairs.parquet/.csv        — per route-pair at a named location
"""

import argparse
from datetime import date
from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DELAY_ROOT = PROJECT_ROOT / "data" / "analysis" / "delay_table"
DEFAULT_STATIC_ROOT = PROJECT_ROOT / "data" / "parsed_static_gtfs"
DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "data" / "analysis" / "transfers"

HAVERSINE = """
    6371000 * 2 * asin(sqrt(
        pow(sin(radians(({to_lat} - {from_lat}) / 2)), 2)
        + cos(radians({from_lat})) * cos(radians({to_lat}))
        * pow(sin(radians(({to_lon} - {from_lon}) / 2)), 2)
    ))
"""


def build_events_for_date(con, delay_root, static_root, snapshot_date, args):
    """Create an `events` table of proposed connections for one date."""
    distance_expr = HAVERSINE.format(
        from_lat="f.stop_lat", from_lon="f.stop_lon",
        to_lat="t.stop_lat", to_lon="t.stop_lon",
    )

    con.sql(f"""
    CREATE OR REPLACE TABLE events AS
    WITH finals AS (
        -- Final pre-arrival observation per trip-stop (the delay table keeps
        -- every snapshot).
        SELECT * EXCLUDE (rn)
        FROM (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY trip_id, stop_id
                    ORDER BY collected_at_utc DESC
                ) AS rn
            FROM read_parquet('{delay_root}/date={snapshot_date}/part-000.parquet')
            WHERE prediction_lead_minutes >= 0
              AND abs(delay_seconds) < 3600
        )
        WHERE rn = 1
    ),

    observations AS (
        SELECT
            f.trip_id,
            f.route_id,
            f.direction_id,
            f.stop_id,
            f.feed_name,
            CASE WHEN f.feed_name LIKE 'bus_%' THEN 'bus' ELSE 'lrt' END AS transit_mode,
            CASE WHEN f.feed_name LIKE 'bus_%' THEN 'bus_static_gtfs' ELSE 'lrt_static_gtfs' END
                AS static_feed,
            f.scheduled_arrival_utc,
            f.predicted_arrival_utc AS actual_arrival_utc,
            COALESCE(f.predicted_departure_utc, f.predicted_arrival_utc) AS actual_departure_utc,
            s.stop_name,
            TRY_CAST(s.stop_lat AS DOUBLE) AS stop_lat,
            TRY_CAST(s.stop_lon AS DOUBLE) AS stop_lon
        FROM finals f
        JOIN (
            SELECT DISTINCT p_feed, stop_id, stop_name, stop_lat, stop_lon
            FROM read_parquet(
                '{static_root}/stops/p_feed=*/p_snapshot_date={snapshot_date}/part-000.parquet',
                hive_partitioning=true)
        ) s
            ON f.stop_id = s.stop_id
            AND CASE WHEN f.feed_name LIKE 'bus_%' THEN 'bus_static_gtfs' ELSE 'lrt_static_gtfs' END
                = s.p_feed
        WHERE TRY_CAST(s.stop_lat AS DOUBLE) IS NOT NULL
          AND TRY_CAST(s.stop_lon AS DOUBLE) IS NOT NULL
    ),

    stops_present AS (
        SELECT DISTINCT stop_id, static_feed, stop_name, stop_lat, stop_lon
        FROM observations
    ),

    stop_pairs AS (
        -- Stops reachable from each other on foot, including the stop itself.
        SELECT
            f.stop_id AS from_stop_id,
            f.static_feed AS from_static_feed,
            t.stop_id AS to_stop_id,
            t.static_feed AS to_static_feed,
            {distance_expr} AS walk_distance_m
        FROM stops_present f
        JOIN stops_present t
            ON abs(f.stop_lat - t.stop_lat) < 0.002
            AND abs(f.stop_lon - t.stop_lon) < 0.003
        WHERE {distance_expr} <= {args.max_walk_m}
    ),

    candidates AS (
        SELECT
            a.trip_id AS from_trip_id,
            a.route_id AS from_route_id,
            a.direction_id AS from_direction_id,
            a.transit_mode AS from_mode,
            a.stop_id AS from_stop_id,
            a.stop_name AS from_stop_name,
            a.stop_lat AS from_stop_lat,
            a.stop_lon AS from_stop_lon,
            a.scheduled_arrival_utc,
            a.actual_arrival_utc,
            b.trip_id AS to_trip_id,
            b.route_id AS to_route_id,
            b.direction_id AS to_direction_id,
            b.transit_mode AS to_mode,
            b.stop_id AS to_stop_id,
            b.stop_name AS to_stop_name,
            b.scheduled_arrival_utc AS scheduled_departure_utc,
            b.actual_departure_utc,
            sp.walk_distance_m,
            greatest(30.0, sp.walk_distance_m / 1.2) AS walk_seconds
        FROM observations a
        JOIN stop_pairs sp
            ON a.stop_id = sp.from_stop_id AND a.static_feed = sp.from_static_feed
        JOIN observations b
            ON b.stop_id = sp.to_stop_id AND b.static_feed = sp.to_static_feed
            AND b.route_id != a.route_id
            AND epoch(b.scheduled_arrival_utc)
                >= epoch(a.scheduled_arrival_utc) + greatest(30.0, sp.walk_distance_m / 1.2)
            AND epoch(b.scheduled_arrival_utc)
                <= epoch(a.scheduled_arrival_utc) + {args.max_wait_minutes * 60}
    ),

    proposed AS (
        -- The connection a planner would propose: the first catchable
        -- scheduled departure per destination route+direction.
        SELECT * EXCLUDE (rn)
        FROM (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY from_trip_id, from_stop_id, to_route_id, to_direction_id
                    ORDER BY scheduled_departure_utc, walk_distance_m
                ) AS rn
            FROM candidates
        )
        WHERE rn = 1
    )

    SELECT
        DATE '{snapshot_date}' AS snapshot_date,
        *,
        epoch(scheduled_departure_utc) - epoch(scheduled_arrival_utc) AS scheduled_wait_seconds,
        epoch(actual_departure_utc) - (epoch(actual_arrival_utc) + walk_seconds)
            AS actual_margin_seconds,
        epoch(actual_departure_utc) >= epoch(actual_arrival_utc) + walk_seconds AS made_transfer
    FROM proposed
    """)

    return con.sql("SELECT count(*) FROM events").fetchone()[0]


def write_events(con, output_root, snapshot_date, overwrite):
    output_dir = output_root / "events" / f"date={snapshot_date}"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "part-000.parquet"
    if output_path.exists() and not overwrite:
        raise FileExistsError(f"{output_path} already exists. Pass --overwrite to replace it.")
    con.sql(f"COPY events TO '{output_path}' (FORMAT PARQUET, COMPRESSION SNAPPY)")
    return output_path


def iso_date(value):
    return date.fromisoformat(value).isoformat()


def build_summaries(con, output_root, min_observations, start_date=None):
    events_glob = f"{output_root}/events/date=*/part-000.parquet"
    date_filter = f"WHERE snapshot_date >= DATE '{start_date}'" if start_date else ""

    con.sql(f"""
    CREATE OR REPLACE TABLE stop_summary AS
    SELECT
        from_mode,
        from_route_id,
        from_direction_id,
        to_mode,
        to_route_id,
        to_direction_id,
        from_stop_id,
        any_value(from_stop_name) AS from_stop_name,
        any_value(from_stop_lat) AS from_stop_lat,
        any_value(from_stop_lon) AS from_stop_lon,
        to_stop_id,
        any_value(to_stop_name) AS to_stop_name,
        any_value(walk_distance_m) AS walk_distance_m,
        count(*) AS attempts,
        sum(CASE WHEN made_transfer THEN 1 ELSE 0 END) AS successes,
        avg(CASE WHEN made_transfer THEN 1.0 ELSE 0.0 END) AS success_rate,
        median(scheduled_wait_seconds) AS median_scheduled_wait_seconds,
        median(actual_margin_seconds) AS median_margin_seconds,
        quantile_cont(actual_margin_seconds, 0.10) AS p10_margin_seconds,
        count(DISTINCT snapshot_date) AS days_observed
    FROM read_parquet('{events_glob}', hive_partitioning=true)
    {date_filter}
    GROUP BY from_mode, from_route_id, from_direction_id,
             to_mode, to_route_id, to_direction_id, from_stop_id, to_stop_id
    HAVING count(*) >= {min_observations}
    ORDER BY success_rate, attempts DESC
    """)

    con.sql(f"""
    CREATE OR REPLACE TABLE route_pairs AS
    SELECT
        from_mode,
        from_route_id,
        to_mode,
        to_route_id,
        from_stop_name AS transfer_stop_name,
        any_value(from_stop_lat) AS stop_lat,
        any_value(from_stop_lon) AS stop_lon,
        count(*) AS attempts,
        sum(CASE WHEN made_transfer THEN 1 ELSE 0 END) AS successes,
        avg(CASE WHEN made_transfer THEN 1.0 ELSE 0.0 END) AS success_rate,
        median(scheduled_wait_seconds) AS median_scheduled_wait_seconds,
        median(actual_margin_seconds) AS median_margin_seconds,
        count(DISTINCT snapshot_date) AS days_observed
    FROM read_parquet('{events_glob}', hive_partitioning=true)
    {date_filter}
    GROUP BY from_mode, from_route_id, to_mode, to_route_id, from_stop_name
    HAVING count(*) >= {min_observations}
    ORDER BY success_rate, attempts DESC
    """)

    for table, name in (("stop_summary", "transfer_stop_summary"), ("route_pairs", "transfer_route_pairs")):
        con.sql(f"COPY {table} TO '{output_root}/{name}.parquet' (FORMAT PARQUET, COMPRESSION SNAPPY)")
        con.sql(f"COPY {table} TO '{output_root}/{name}.csv' (FORMAT CSV, HEADER)")
        count = con.sql(f"SELECT count(*) FROM {table}").fetchone()[0]
        print(f"Wrote {count} rows to {output_root}/{name}.parquet (+ .csv)")


def discover_dates(delay_root):
    if not delay_root.exists():
        return []
    return [
        entry.name.removeprefix("date=")
        for entry in sorted(delay_root.iterdir())
        if entry.is_dir() and entry.name.startswith("date=")
    ]


def parse_args():
    parser = argparse.ArgumentParser(
        description="Compute transfer reliability from the delay table and static GTFS stops."
    )
    parser.add_argument("--delay-root", type=Path, default=DEFAULT_DELAY_ROOT,
                        help="Root of delay table Parquet files (all-snapshot or final).")
    parser.add_argument("--static-root", type=Path, default=DEFAULT_STATIC_ROOT,
                        help="Root of parsed static GTFS Parquet tables.")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT,
                        help="Output directory for transfer events and summaries.")
    parser.add_argument("--date", action="append",
                        help="Date to process in YYYY-MM-DD form. May be repeated. Defaults to all.")
    parser.add_argument("--start-date", type=iso_date,
                        help="Exclude connection events before this date (YYYY-MM-DD).")
    parser.add_argument("--max-walk-m", type=float, default=150.0,
                        help="Maximum walking distance between transfer stops in metres.")
    parser.add_argument("--max-wait-minutes", type=int, default=30,
                        help="Maximum scheduled wait for a proposed connection.")
    parser.add_argument("--min-observations", type=int, default=10,
                        help="Minimum attempts for a connection to appear in summaries.")
    parser.add_argument("--overwrite", action="store_true", help="Replace existing outputs.")
    parser.add_argument("--summaries-only", action="store_true",
                        help="Skip event building and just rebuild summaries from existing events.")
    return parser.parse_args()


def main():
    args = parse_args()
    delay_root = args.delay_root.resolve()
    static_root = args.static_root.resolve()
    output_root = args.output_root.resolve()
    output_root.mkdir(parents=True, exist_ok=True)

    if not args.summaries_only:
        dates = args.date if args.date else discover_dates(delay_root)
        if args.start_date:
            dates = [d for d in dates if d >= args.start_date]
        if not dates:
            print(f"No dates found under {delay_root}")
            raise SystemExit(1)

        print(f"Building transfer events for {len(dates)} dates...")
        for snapshot_date in dates:
            con = duckdb.connect()
            con.sql("SET timezone = 'UTC'")
            con.sql("SET preserve_insertion_order = false")
            try:
                count = build_events_for_date(con, delay_root, static_root, snapshot_date, args)
            except duckdb.IOException as error:
                print(f"  {snapshot_date}: skipped ({error})")
                con.close()
                continue
            output_path = write_events(con, output_root, snapshot_date, args.overwrite)
            print(f"  {snapshot_date}: {count} proposed connections -> {output_path}")
            con.close()

    con = duckdb.connect()
    con.sql("SET timezone = 'UTC'")
    print("\nBuilding summaries...")
    build_summaries(con, output_root, args.min_observations, args.start_date)

    stats_filter = f"WHERE snapshot_date >= DATE '{args.start_date}'" if args.start_date else ""
    stats = con.sql(f"""
        SELECT count(*), avg(CASE WHEN made_transfer THEN 1.0 ELSE 0.0 END),
               median(actual_margin_seconds)
        FROM read_parquet('{output_root}/events/date=*/part-000.parquet', hive_partitioning=true)
        {stats_filter}
    """).fetchone()
    print(f"\nOverall: {stats[0]} proposed connections, "
          f"{stats[1]*100:.1f}% made, median margin {stats[2]:.0f}s")
    con.close()


if __name__ == "__main__":
    main()
