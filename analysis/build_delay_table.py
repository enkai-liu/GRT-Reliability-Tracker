"""Compute delay_seconds by joining realtime stop_time_updates with static GTFS stop_times.

The GRT realtime feed provides predicted arrival/departure times (arrival_time_utc,
departure_time_utc) but NOT delay values (arrival_delay_seconds is always null).
Delays are computed as: realtime predicted time - scheduled time (converted to UTC).

Static GTFS times are local Eastern Time strings like "19:00:00" (or >24h for
midnight-crossing trips). Combined with the realtime start_date ("20260513"),
we derive the scheduled UTC timestamp for comparison.

Output: data/analysis/delay_table/date=YYYY-MM-DD/part-000.parquet
"""

import argparse
from datetime import datetime
from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PARSED_ROOT = PROJECT_ROOT / "data" / "parsed"
DEFAULT_STATIC_ROOT = PROJECT_ROOT / "data" / "parsed_static_gtfs"
DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "data" / "analysis" / "delay_table"

# Feed name prefix → static GTFS feed name mapping
FEED_MAPPING = {
    "bus": "bus_static_gtfs",
    "lrt": "lrt_static_gtfs",
}


def build_delay_table(con, parsed_root, static_root, dates, overwrite):
    date_filter = ""
    if dates:
        date_list = ", ".join(f"'{d}'" for d in dates)
        date_filter = f"AND snapshot_date IN ({date_list})"

    # DuckDB reads Parquet with Hive partitioning natively.
    # We build one big query that:
    #   1. Reads realtime stop_time_updates (filtering to rows with arrival_time_utc)
    #   2. Determines the static feed (bus vs lrt) from feed_name prefix
    #   3. Joins to static stop_times on (trip_id, stop_id, stop_sequence)
    #   4. Converts static local time + start_date → scheduled UTC timestamp
    #   5. Computes delay_seconds = predicted - scheduled
    #   6. Computes prediction_lead_minutes = (predicted arrival - collected_at) / 60

    query = f"""
    WITH realtime AS (
        SELECT
            feed_name,
            snapshot_date,
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
            departure_time_utc,
            -- Map feed_name prefix to static feed
            CASE
                WHEN feed_name LIKE 'bus_%' THEN 'bus_static_gtfs'
                WHEN feed_name LIKE 'lrt_%' THEN 'lrt_static_gtfs'
            END AS static_feed
        FROM read_parquet('{parsed_root}/stop_time_updates/date=*/part-000.parquet',
                          hive_partitioning=true)
        WHERE arrival_time_utc IS NOT NULL
        {date_filter}
    ),

    static_stop_times AS (
        SELECT
            p_feed,
            p_snapshot_date,
            trip_id,
            stop_id,
            CAST(stop_sequence AS INTEGER) AS stop_sequence,
            arrival_time,
            departure_time
        FROM read_parquet('{static_root}/stop_times/p_feed=*/p_snapshot_date=*/part-000.parquet',
                          hive_partitioning=true)
    ),

    joined AS (
        SELECT
            r.snapshot_date,
            r.collected_at_utc,
            r.feed_name,
            r.trip_id,
            r.route_id,
            r.direction_id,
            r.vehicle_id,
            r.stop_id,
            r.stop_sequence,
            r.arrival_time_utc AS predicted_arrival_utc,
            r.departure_time_utc AS predicted_departure_utc,

            -- Parse start_date "20260513" into a DATE
            strptime(r.start_date, '%Y%m%d')::DATE AS service_date,

            -- Parse static arrival_time (e.g. "19:00:00" or "25:30:00")
            -- Hours can exceed 24 for trips crossing midnight
            s.arrival_time AS scheduled_arrival_local_str,

            -- Build scheduled UTC timestamp:
            -- 1. Extract hours/minutes/seconds from the static time string
            -- 2. Add to service_date as an interval (handles >24h correctly)
            -- 3. Interpret as America/Toronto, convert to UTC
            --
            -- Midnight-crossing fix: The static GTFS uses hours >= 24 for
            -- post-midnight stops on a service day. The realtime feed sometimes
            -- reports these with start_date = next calendar day and start_time
            -- wrapped to 0-23h. When static hour >= 24 and realtime start_time
            -- hour < 6, subtract 1 day from start_date to get the true service date.
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
                    -- LRT realtime stop_sequence is one-based; static GTFS is zero-based.
                    WHEN r.static_feed = 'lrt_static_gtfs' THEN r.stop_sequence - 1
                    ELSE r.stop_sequence
                END = s.stop_sequence
            AND r.static_feed = s.p_feed
            AND r.snapshot_date = s.p_snapshot_date
    ),

    with_delay AS (
        SELECT
            snapshot_date,
            collected_at_utc,
            feed_name,
            trip_id,
            route_id,
            direction_id,
            vehicle_id,
            stop_id,
            stop_sequence,
            service_date,
            scheduled_arrival_local_str,
            scheduled_arrival_utc,
            predicted_arrival_utc,
            predicted_departure_utc,

            -- Delay = predicted - scheduled (positive = late)
            epoch(predicted_arrival_utc) - epoch(scheduled_arrival_utc) AS delay_seconds,

            -- How far in advance this prediction was made
            (epoch(predicted_arrival_utc) - epoch(collected_at_utc)) / 60.0
                AS prediction_lead_minutes

        FROM joined
        WHERE scheduled_arrival_utc IS NOT NULL
    )

    SELECT * FROM with_delay
    ORDER BY snapshot_date, trip_id, stop_sequence, collected_at_utc
    """

    return con.sql(query)


def deduplicate_to_final_prediction(con, full_table_name):
    """For each (trip_id, stop_id, snapshot_date), keep only the last prediction
    collected before the predicted arrival time. This gives the most accurate
    "final" delay observation per trip-stop."""

    query = f"""
    WITH ranked AS (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY snapshot_date, trip_id, stop_id
                ORDER BY collected_at_utc DESC
            ) AS rn
        FROM {full_table_name}
        WHERE prediction_lead_minutes >= 0  -- only predictions before arrival
    )
    SELECT * EXCLUDE (rn)
    FROM ranked
    WHERE rn = 1
    """
    return con.sql(query)


def write_output(con, result, output_root, dates, overwrite):
    output_paths = []

    if dates:
        date_list = dates
    else:
        date_list = [
            row[0]
            for row in con.sql(
                f"SELECT DISTINCT snapshot_date FROM result ORDER BY snapshot_date"
            ).fetchall()
        ]

    for snapshot_date in date_list:
        output_dir = output_root / f"date={snapshot_date}"
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / "part-000.parquet"

        if output_path.exists() and not overwrite:
            raise FileExistsError(f"{output_path} already exists. Pass --overwrite to replace it.")

        con.sql(f"""
            COPY (
                SELECT * FROM result WHERE snapshot_date = '{snapshot_date}'
            ) TO '{output_path}' (FORMAT PARQUET, COMPRESSION SNAPPY)
        """)

        row_count = con.sql(
            f"SELECT count(*) FROM result WHERE snapshot_date = '{snapshot_date}'"
        ).fetchone()[0]
        output_paths.append((output_path, row_count))
        print(f"Wrote {row_count} rows to {output_path}")

    return output_paths


def parse_args():
    parser = argparse.ArgumentParser(
        description="Compute delay table by joining realtime predictions with static GTFS schedule."
    )
    parser.add_argument(
        "--parsed-root", type=Path, default=DEFAULT_PARSED_ROOT,
        help="Root of parsed realtime Parquet tables.",
    )
    parser.add_argument(
        "--static-root", type=Path, default=DEFAULT_STATIC_ROOT,
        help="Root of parsed static GTFS Parquet tables.",
    )
    parser.add_argument(
        "--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT,
        help="Output directory for delay table Parquet.",
    )
    parser.add_argument(
        "--date", action="append",
        help="Date to process in YYYY-MM-DD form. May be repeated. Defaults to all available.",
    )
    parser.add_argument("--overwrite", action="store_true", help="Replace existing outputs.")
    parser.add_argument(
        "--keep-all-snapshots", action="store_true",
        help="Keep all snapshot observations instead of deduplicating to final prediction only.",
    )
    return parser.parse_args()


def discover_dates(parsed_root):
    """Find all available date partitions in the stop_time_updates directory."""
    stu_dir = parsed_root / "stop_time_updates"
    if not stu_dir.exists():
        return []
    dates = []
    for entry in sorted(stu_dir.iterdir()):
        if entry.is_dir() and entry.name.startswith("date="):
            dates.append(entry.name.removeprefix("date="))
    return dates


def process_single_date(parsed_root, static_root, output_root, snapshot_date, overwrite, keep_all):
    """Process a single date end-to-end in its own DuckDB connection."""
    con = duckdb.connect()
    con.sql("SET timezone = 'UTC'")

    full_result = build_delay_table(con, parsed_root, static_root, [snapshot_date], overwrite)
    con.sql("CREATE TABLE full_delays AS SELECT * FROM full_result")

    total = con.sql("SELECT count(*) FROM full_delays").fetchone()[0]
    if total == 0:
        print(f"  {snapshot_date}: no joined rows, skipping")
        con.close()
        return 0

    if keep_all:
        con.sql("CREATE VIEW result AS SELECT * FROM full_delays")
    else:
        deduped = deduplicate_to_final_prediction(con, "full_delays")
        con.sql("CREATE TABLE result AS SELECT * FROM deduped")

    final_count = con.sql("SELECT count(*) FROM result").fetchone()[0]

    output_dir = output_root / f"date={snapshot_date}"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "part-000.parquet"

    if output_path.exists() and not overwrite:
        raise FileExistsError(f"{output_path} already exists. Pass --overwrite to replace it.")

    con.sql(f"COPY (SELECT * FROM result) TO '{output_path}' (FORMAT PARQUET, COMPRESSION SNAPPY)")
    print(f"  {snapshot_date}: {total} joined -> {final_count} deduplicated -> {output_path}")

    con.close()
    return final_count


def main():
    args = parse_args()
    parsed_root = args.parsed_root.resolve()
    static_root = args.static_root.resolve()
    output_root = args.output_root.resolve()

    dates = args.date if args.date else discover_dates(parsed_root)
    if not dates:
        print(f"No dates found under {parsed_root}")
        raise SystemExit(1)

    print(f"Building delay table for {len(dates)} dates ({dates[0]} to {dates[-1]})...")
    total_rows = 0
    for snapshot_date in dates:
        total_rows += process_single_date(
            parsed_root, static_root, output_root, snapshot_date,
            args.overwrite, args.keep_all_snapshots,
        )

    print(f"\nDone. Total: {total_rows} rows across {len(dates)} dates.")

    # Summary stats across all output files
    con = duckdb.connect()
    con.sql("SET timezone = 'UTC'")
    stats = con.sql(f"""
        SELECT
            count(*) AS rows,
            count(DISTINCT trip_id) AS unique_trips,
            count(DISTINCT stop_id) AS unique_stops,
            count(DISTINCT route_id) AS unique_routes,
            avg(delay_seconds) AS avg_delay_s,
            median(delay_seconds) AS median_delay_s,
            percentile_cont(0.95) WITHIN GROUP (ORDER BY delay_seconds) AS p95_delay_s,
            min(delay_seconds) AS min_delay_s,
            max(delay_seconds) AS max_delay_s
        FROM read_parquet('{output_root}/date=*/part-000.parquet', hive_partitioning=true)
    """).fetchone()
    print(f"Summary:")
    print(f"  Rows: {stats[0]}, Trips: {stats[1]}, Stops: {stats[2]}, Routes: {stats[3]}")
    print(f"  Delay (seconds): avg={stats[4]:.0f}, median={stats[5]:.0f}, "
          f"p95={stats[6]:.0f}, min={stats[7]:.0f}, max={stats[8]:.0f}")


if __name__ == "__main__":
    main()
