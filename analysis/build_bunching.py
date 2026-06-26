"""Detect bus bunching from observed headways in the feature table.

For each route/direction/stop, trips are ordered by scheduled arrival and the
headway behind the preceding vehicle is computed twice: as scheduled and as
observed (final pre-arrival GTFS-RT predictions, same source as the
reliability tables). The headway ratio (observed / scheduled) classifies each
arrival:

  - bunched:    ratio < 0.5  (vehicle arrived on the heels of its leader,
                              including overtakes, where the ratio is negative)
  - on-headway: 0.5 <= ratio <= 1.5
  - gapped:     ratio > 1.5  (the rider-facing cost of the bunch ahead)

Only frequent service is considered (scheduled headway between
--min-scheduled-headway-seconds and --max-scheduled-headway-minutes); bunching
is not meaningful for hourly routes.

Excess wait time (EWT) per group estimates how much longer a randomly arriving
rider waits versus perfectly even service: E[h^2] / 2E[h] computed over
observed minus scheduled headways.

Outputs under data/analysis/bunching by default:
  - events/date=YYYY-MM-DD/part-000.parquet  — one row per observed headway
  - system_by_date.{parquet,csv}
  - route_summary.{parquet,csv}
  - route_by_hour.{parquet,csv}
  - stop_summary.{parquet,csv}
  - bunching_report.md
"""

import argparse
from datetime import date
from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_FEATURES_ROOT = PROJECT_ROOT / "data" / "analysis" / "features"
DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "data" / "analysis" / "bunching"


def iso_date(value):
    return date.fromisoformat(value).isoformat()


def sql_list(values):
    return ", ".join(f"'{value}'" for value in values)


def create_headway_view(con, features_root, args):
    filters = []
    if args.date:
        filters.append(f"AND snapshot_date IN ({sql_list(args.date)})")
    if args.start_date:
        # snapshot_date is stored as an ISO string, so string comparison is safe
        filters.append(f"AND snapshot_date >= '{args.start_date}'")
    date_filter = "\n              ".join(filters)

    min_headway_s = args.min_scheduled_headway_seconds
    max_headway_s = args.max_scheduled_headway_minutes * 60

    con.sql(f"""
        CREATE OR REPLACE VIEW headways AS
        WITH arrivals AS (
            SELECT
                snapshot_date,
                transit_mode,
                feed_name,
                route_id,
                COALESCE(route_short_name, route_id) AS route_short_name,
                direction_id,
                trip_id,
                stop_id,
                COALESCE(stop_name, stop_id) AS stop_name,
                stop_lat,
                stop_lon,
                hour_of_day,
                day_of_week,
                is_weekend,
                is_rush_hour,
                scheduled_arrival_utc,
                predicted_arrival_utc AS actual_arrival_utc
            FROM read_parquet('{features_root}/date=*/part-000.parquet', hive_partitioning=true)
            WHERE delay_seconds IS NOT NULL
              AND abs(delay_seconds) <= {args.max_abs_delay_seconds}
              AND predicted_arrival_utc IS NOT NULL
              {date_filter}
        ),

        sequenced AS (
            SELECT
                *,
                LAG(trip_id) OVER w AS leader_trip_id,
                LAG(scheduled_arrival_utc) OVER w AS leader_scheduled_arrival_utc,
                LAG(actual_arrival_utc) OVER w AS leader_actual_arrival_utc
            FROM arrivals
            WINDOW w AS (
                PARTITION BY snapshot_date, route_id, direction_id, stop_id
                ORDER BY scheduled_arrival_utc, trip_id
            )
        )

        SELECT
            * EXCLUDE (leader_scheduled_arrival_utc, leader_actual_arrival_utc),
            date_diff('second', leader_scheduled_arrival_utc, scheduled_arrival_utc)
                AS scheduled_headway_seconds,
            date_diff('second', leader_actual_arrival_utc, actual_arrival_utc)
                AS actual_headway_seconds,
            date_diff('second', leader_actual_arrival_utc, actual_arrival_utc)
                / date_diff('second', leader_scheduled_arrival_utc, scheduled_arrival_utc)::DOUBLE
                AS headway_ratio
        FROM sequenced
        WHERE leader_trip_id IS NOT NULL
          AND date_diff('second', leader_scheduled_arrival_utc, scheduled_arrival_utc)
              BETWEEN {min_headway_s} AND {max_headway_s}
          AND abs(date_diff('second', leader_actual_arrival_utc, actual_arrival_utc)) <= 7200
    """)

    con.sql(f"""
        CREATE OR REPLACE VIEW bunching_base AS
        SELECT
            *,
            headway_ratio < {args.bunched_ratio} AS is_bunched,
            headway_ratio > {args.gapped_ratio} AS is_gapped,
            headway_ratio BETWEEN {args.bunched_ratio} AND {args.gapped_ratio} AS is_on_headway
        FROM headways
    """)


# EWT compares the expected wait of a randomly arriving rider (E[h^2] / 2E[h])
# under observed vs scheduled headways; overtakes are clamped to zero so they
# do not contribute negative squared gaps.
METRIC_SELECT = """
    count(*) AS observations,
    count(DISTINCT snapshot_date) AS service_days,
    avg(scheduled_headway_seconds) AS avg_scheduled_headway_seconds,
    avg(actual_headway_seconds) AS avg_actual_headway_seconds,
    median(headway_ratio) AS median_headway_ratio,
    avg(CASE WHEN is_bunched THEN 1.0 ELSE 0.0 END) AS bunched_rate,
    avg(CASE WHEN is_gapped THEN 1.0 ELSE 0.0 END) AS gapped_rate,
    avg(CASE WHEN is_on_headway THEN 1.0 ELSE 0.0 END) AS on_headway_rate,
    sum(pow(greatest(actual_headway_seconds, 0), 2)) / (2 * sum(greatest(actual_headway_seconds, 0)))
        - sum(pow(scheduled_headway_seconds, 2)) / (2 * sum(scheduled_headway_seconds))
        AS excess_wait_seconds
"""


TABLE_QUERIES = {
    "system_by_date": f"""
        SELECT
            snapshot_date,
            {METRIC_SELECT}
        FROM bunching_base
        GROUP BY snapshot_date
        ORDER BY snapshot_date
    """,
    "route_summary": f"""
        SELECT
            transit_mode,
            route_id,
            route_short_name,
            {METRIC_SELECT}
        FROM bunching_base
        GROUP BY transit_mode, route_id, route_short_name
        ORDER BY transit_mode, route_short_name
    """,
    "route_by_hour": f"""
        SELECT
            transit_mode,
            route_id,
            route_short_name,
            hour_of_day,
            {METRIC_SELECT}
        FROM bunching_base
        GROUP BY transit_mode, route_id, route_short_name, hour_of_day
        ORDER BY transit_mode, route_short_name, hour_of_day
    """,
    "stop_summary": f"""
        SELECT
            transit_mode,
            route_id,
            route_short_name,
            direction_id,
            stop_id,
            stop_name,
            avg(stop_lat) AS stop_lat,
            avg(stop_lon) AS stop_lon,
            {METRIC_SELECT}
        FROM bunching_base
        GROUP BY transit_mode, route_id, route_short_name, direction_id, stop_id, stop_name
        ORDER BY transit_mode, route_short_name, direction_id, stop_name
    """,
}


def write_events(con, output_root):
    dates = [row[0] for row in con.sql(
        "SELECT DISTINCT snapshot_date FROM bunching_base ORDER BY snapshot_date"
    ).fetchall()]
    for snapshot_date in dates:
        out_dir = output_root / "events" / f"date={snapshot_date}"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / "part-000.parquet"
        con.sql(f"""
            COPY (SELECT * FROM bunching_base WHERE snapshot_date = '{snapshot_date}')
            TO '{out_path}' (FORMAT PARQUET, COMPRESSION SNAPPY)
        """)
    print(f"Wrote events for {len(dates)} dates under {output_root / 'events'}")


def write_table(con, output_root, table_name, query):
    con.sql(f"CREATE OR REPLACE TABLE {table_name} AS {query}")

    parquet_path = output_root / f"{table_name}.parquet"
    csv_path = output_root / f"{table_name}.csv"
    con.sql(f"COPY {table_name} TO '{parquet_path}' (FORMAT PARQUET, COMPRESSION SNAPPY)")
    con.sql(f"COPY {table_name} TO '{csv_path}' (HEADER, DELIMITER ',')")

    row_count = con.sql(f"SELECT count(*) FROM {table_name}").fetchone()[0]
    print(f"Wrote {row_count} rows to {parquet_path} and {csv_path}")


def pct(value):
    return f"{value * 100:.1f}%"


def seconds(value):
    return f"{value:.0f}s"


def markdown_table(headers, rows):
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
    ]
    for row in rows:
        lines.append("| " + " | ".join(str(value) for value in row) + " |")
    return "\n".join(lines)


def build_report(con, output_root, args):
    overall = con.sql("""
        SELECT
            count(*) AS observations,
            count(DISTINCT snapshot_date) AS service_days,
            min(snapshot_date) AS start_date,
            max(snapshot_date) AS end_date,
            count(DISTINCT route_id) AS routes,
            count(DISTINCT stop_id) AS stops,
            avg(CASE WHEN is_bunched THEN 1.0 ELSE 0.0 END) AS bunched_rate,
            avg(CASE WHEN is_gapped THEN 1.0 ELSE 0.0 END) AS gapped_rate,
            median(headway_ratio) AS median_headway_ratio,
            avg(scheduled_headway_seconds) AS avg_scheduled_headway_seconds
        FROM bunching_base
    """).fetchone()

    mode_rows = con.sql("""
        SELECT
            transit_mode,
            count(*) AS observations,
            count(DISTINCT route_id) AS routes,
            avg(CASE WHEN is_bunched THEN 1.0 ELSE 0.0 END) AS bunched_rate,
            avg(CASE WHEN is_gapped THEN 1.0 ELSE 0.0 END) AS gapped_rate,
            avg(scheduled_headway_seconds) AS avg_scheduled_headway_seconds
        FROM bunching_base
        GROUP BY transit_mode
        ORDER BY transit_mode
    """).fetchall()

    worst_routes = con.sql(f"""
        SELECT
            route_short_name,
            transit_mode,
            observations,
            avg_scheduled_headway_seconds,
            bunched_rate,
            gapped_rate,
            excess_wait_seconds
        FROM route_summary
        WHERE observations >= {args.min_observations}
        ORDER BY bunched_rate DESC
        LIMIT 15
    """).fetchall()

    worst_stops = con.sql(f"""
        SELECT
            route_short_name,
            stop_name,
            direction_id,
            observations,
            bunched_rate,
            gapped_rate,
            excess_wait_seconds
        FROM stop_summary
        WHERE observations >= {args.min_observations / 4}
        ORDER BY bunched_rate DESC
        LIMIT 15
    """).fetchall()

    worst_hours = con.sql(f"""
        SELECT
            route_short_name,
            hour_of_day,
            observations,
            bunched_rate,
            gapped_rate,
            excess_wait_seconds
        FROM route_by_hour
        WHERE observations >= {args.min_observations / 4}
        ORDER BY bunched_rate DESC
        LIMIT 15
    """).fetchall()

    lines = [
        "# GRT Bus Bunching Report",
        "",
        "Source: `data/analysis/features/date=*/part-000.parquet`",
        f"Date range: `{overall[2]}` to `{overall[3]}`",
        "",
        "Definitions (per arrival, versus the vehicle ahead on the same",
        "route/direction/stop):",
        "",
        f"- Bunched: observed headway below {args.bunched_ratio:g}x the scheduled headway",
        f"- Gapped: observed headway above {args.gapped_ratio:g}x the scheduled headway",
        f"- Scope: scheduled headways of {args.min_scheduled_headway_seconds}s to "
        f"{args.max_scheduled_headway_minutes} minutes",
        "- Excess wait: extra average wait for a randomly arriving rider versus",
        "  the schedule, from observed headway variability",
        "",
        "## System Summary",
        "",
        markdown_table(
            [
                "Headway observations",
                "Days",
                "Routes",
                "Stops",
                "Bunched",
                "Gapped",
                "Median headway ratio",
                "Avg scheduled headway",
            ],
            [[
                overall[0],
                overall[1],
                overall[4],
                overall[5],
                pct(overall[6]),
                pct(overall[7]),
                f"{overall[8]:.2f}",
                seconds(overall[9]),
            ]],
        ),
        "",
        "## By Mode",
        "",
        markdown_table(
            ["Mode", "Observations", "Routes", "Bunched", "Gapped", "Avg scheduled headway"],
            [
                [mode, obs, routes, pct(bunched), pct(gapped), seconds(headway)]
                for mode, obs, routes, bunched, gapped, headway in mode_rows
            ],
        ),
        "",
        f"## Most Bunched Routes (min {args.min_observations} observations)",
        "",
        markdown_table(
            ["Route", "Mode", "Observations", "Scheduled headway", "Bunched", "Gapped", "Excess wait"],
            [
                [route, mode, obs, seconds(headway), pct(bunched), pct(gapped), seconds(ewt)]
                for route, mode, obs, headway, bunched, gapped, ewt in worst_routes
            ],
        ),
        "",
        f"## Most Bunched Route-Stops (min {args.min_observations // 4} observations)",
        "",
        markdown_table(
            ["Route", "Stop", "Direction", "Observations", "Bunched", "Gapped", "Excess wait"],
            [
                [route, stop, direction, obs, pct(bunched), pct(gapped), seconds(ewt)]
                for route, stop, direction, obs, bunched, gapped, ewt in worst_stops
            ],
        ),
        "",
        f"## Most Bunched Route-Hours (min {args.min_observations // 4} observations)",
        "",
        markdown_table(
            ["Route", "Hour", "Observations", "Bunched", "Gapped", "Excess wait"],
            [
                [route, hour, obs, pct(bunched), pct(gapped), seconds(ewt)]
                for route, hour, obs, bunched, gapped, ewt in worst_hours
            ],
        ),
        "",
        "## Generated Tables",
        "",
        "- `system_by_date.csv` / `system_by_date.parquet`",
        "- `route_summary.csv` / `route_summary.parquet`",
        "- `route_by_hour.csv` / `route_by_hour.parquet`",
        "- `stop_summary.csv` / `stop_summary.parquet`",
        "- `events/date=YYYY-MM-DD/part-000.parquet`",
        "",
    ]

    report_path = output_root / "bunching_report.md"
    report_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"Wrote {report_path}")


def parse_args():
    parser = argparse.ArgumentParser(description="Detect bus bunching from observed headways.")
    parser.add_argument(
        "--features-root",
        type=Path,
        default=DEFAULT_FEATURES_ROOT,
        help="Root directory of feature table Parquet files.",
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=DEFAULT_OUTPUT_ROOT,
        help="Output directory for bunching tables.",
    )
    parser.add_argument(
        "--date",
        action="append",
        help="Date to include in YYYY-MM-DD form. May be repeated. Defaults to all dates.",
    )
    parser.add_argument(
        "--start-date",
        type=iso_date,
        help="Exclude observations before this date (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--max-abs-delay-seconds",
        type=int,
        default=3600,
        help="Exclude observations with absolute delay above this threshold.",
    )
    parser.add_argument(
        "--min-scheduled-headway-seconds",
        type=int,
        default=120,
        help="Ignore headways scheduled tighter than this (interlined duplicates).",
    )
    parser.add_argument(
        "--max-scheduled-headway-minutes",
        type=int,
        default=30,
        help="Ignore headways scheduled wider than this (bunching needs frequent service).",
    )
    parser.add_argument(
        "--bunched-ratio",
        type=float,
        default=0.5,
        help="Observed/scheduled headway ratio below which an arrival is bunched.",
    )
    parser.add_argument(
        "--gapped-ratio",
        type=float,
        default=1.5,
        help="Observed/scheduled headway ratio above which an arrival is gapped.",
    )
    parser.add_argument(
        "--min-observations",
        type=int,
        default=1000,
        help="Minimum observations for worst-route report sections (stop and hour sections use a quarter of this).",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    features_root = args.features_root.resolve()
    output_root = args.output_root.resolve()
    output_root.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    con.sql("SET timezone = 'UTC'")

    create_headway_view(con, features_root, args)
    observations = con.sql("SELECT count(*) FROM bunching_base").fetchone()[0]
    if observations == 0:
        print("No headway observations found for bunching tables.")
        raise SystemExit(1)

    print(f"Building bunching tables from {observations} headway observations...")
    write_events(con, output_root)
    for table_name, query in TABLE_QUERIES.items():
        write_table(con, output_root, table_name, query)

    build_report(con, output_root, args)


if __name__ == "__main__":
    main()
