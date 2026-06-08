"""Build reliability summary tables from enriched trip-stop delay features.

The input feature table has one final delay observation per trip-stop. This
script rolls those observations up into dashboard-friendly tables and a Markdown
summary report.

Default reliability definitions:
  - early: delay_seconds < -60
  - on-time: -60 <= delay_seconds <= 300
  - late: delay_seconds > 300

Outputs under data/analysis/reliability by default:
  - system_by_date.{parquet,csv}
  - route_summary.{parquet,csv}
  - route_by_hour.{parquet,csv}
  - route_by_day_of_week.{parquet,csv}
  - stop_summary.{parquet,csv}
  - route_stop_summary.{parquet,csv}
  - dashboard_report.md
"""

import argparse
from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_FEATURES_ROOT = PROJECT_ROOT / "data" / "analysis" / "features"
DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "data" / "analysis" / "reliability"


def sql_list(values):
    return ", ".join(f"'{value}'" for value in values)


def create_base_view(con, features_root, dates, max_abs_delay_seconds):
    date_filter = ""
    if dates:
        date_filter = f"AND snapshot_date IN ({sql_list(dates)})"

    con.sql(f"""
        CREATE OR REPLACE VIEW reliability_base AS
        SELECT
            snapshot_date,
            transit_mode,
            feed_name,
            route_id,
            COALESCE(route_short_name, route_id) AS route_short_name,
            stop_id,
            COALESCE(stop_name, stop_id) AS stop_name,
            stop_lat,
            stop_lon,
            direction_id,
            trip_id,
            scheduled_arrival_utc,
            hour_of_day,
            day_of_week,
            CASE day_of_week
                WHEN 0 THEN 'Sunday'
                WHEN 1 THEN 'Monday'
                WHEN 2 THEN 'Tuesday'
                WHEN 3 THEN 'Wednesday'
                WHEN 4 THEN 'Thursday'
                WHEN 5 THEN 'Friday'
                WHEN 6 THEN 'Saturday'
            END AS day_name,
            is_weekend,
            is_rush_hour,
            delay_seconds,
            delay_seconds < -60 AS is_early,
            delay_seconds BETWEEN -60 AND 300 AS is_on_time,
            delay_seconds > 300 AS is_late
        FROM read_parquet('{features_root}/date=*/part-000.parquet', hive_partitioning=true)
        WHERE delay_seconds IS NOT NULL
          AND abs(delay_seconds) <= {max_abs_delay_seconds}
          {date_filter}
    """)


METRIC_SELECT = """
    count(*) AS observations,
    count(DISTINCT snapshot_date) AS service_days,
    count(DISTINCT trip_id) AS trips,
    avg(delay_seconds) AS avg_delay_seconds,
    median(delay_seconds) AS median_delay_seconds,
    quantile_cont(delay_seconds, 0.75) AS p75_delay_seconds,
    quantile_cont(delay_seconds, 0.90) AS p90_delay_seconds,
    quantile_cont(delay_seconds, 0.95) AS p95_delay_seconds,
    avg(abs(delay_seconds)) AS avg_abs_delay_seconds,
    avg(CASE WHEN is_on_time THEN 1.0 ELSE 0.0 END) AS on_time_rate,
    avg(CASE WHEN is_late THEN 1.0 ELSE 0.0 END) AS late_rate,
    avg(CASE WHEN is_early THEN 1.0 ELSE 0.0 END) AS early_rate
"""


TABLE_QUERIES = {
    "system_by_date": f"""
        SELECT
            snapshot_date,
            {METRIC_SELECT}
        FROM reliability_base
        GROUP BY snapshot_date
        ORDER BY snapshot_date
    """,
    "route_summary": f"""
        SELECT
            transit_mode,
            route_id,
            route_short_name,
            {METRIC_SELECT}
        FROM reliability_base
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
        FROM reliability_base
        GROUP BY transit_mode, route_id, route_short_name, hour_of_day
        ORDER BY transit_mode, route_short_name, hour_of_day
    """,
    "route_by_day_of_week": f"""
        SELECT
            transit_mode,
            route_id,
            route_short_name,
            day_of_week,
            day_name,
            {METRIC_SELECT}
        FROM reliability_base
        GROUP BY transit_mode, route_id, route_short_name, day_of_week, day_name
        ORDER BY transit_mode, route_short_name, day_of_week
    """,
    "stop_summary": f"""
        SELECT
            transit_mode,
            stop_id,
            stop_name,
            avg(stop_lat) AS stop_lat,
            avg(stop_lon) AS stop_lon,
            {METRIC_SELECT}
        FROM reliability_base
        GROUP BY transit_mode, stop_id, stop_name
        ORDER BY transit_mode, stop_name
    """,
    "route_stop_summary": f"""
        SELECT
            transit_mode,
            route_id,
            route_short_name,
            stop_id,
            stop_name,
            direction_id,
            avg(stop_lat) AS stop_lat,
            avg(stop_lon) AS stop_lon,
            {METRIC_SELECT}
        FROM reliability_base
        GROUP BY transit_mode, route_id, route_short_name, stop_id, stop_name, direction_id
        ORDER BY transit_mode, route_short_name, direction_id, stop_name
    """,
}


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


def build_report(con, output_root, min_observations):
    overall = con.sql("""
        SELECT
            count(*) AS observations,
            count(DISTINCT snapshot_date) AS service_days,
            min(snapshot_date) AS start_date,
            max(snapshot_date) AS end_date,
            count(DISTINCT route_id) AS routes,
            count(DISTINCT stop_id) AS stops,
            avg(delay_seconds) AS avg_delay_seconds,
            median(delay_seconds) AS median_delay_seconds,
            quantile_cont(delay_seconds, 0.90) AS p90_delay_seconds,
            avg(CASE WHEN is_on_time THEN 1.0 ELSE 0.0 END) AS on_time_rate,
            avg(CASE WHEN is_late THEN 1.0 ELSE 0.0 END) AS late_rate,
            avg(CASE WHEN is_early THEN 1.0 ELSE 0.0 END) AS early_rate
        FROM reliability_base
    """).fetchone()

    mode_rows = con.sql("""
        SELECT
            transit_mode,
            count(*) AS observations,
            count(DISTINCT route_id) AS routes,
            avg(CASE WHEN is_on_time THEN 1.0 ELSE 0.0 END) AS on_time_rate,
            median(delay_seconds) AS median_delay_seconds,
            quantile_cont(delay_seconds, 0.90) AS p90_delay_seconds
        FROM reliability_base
        GROUP BY transit_mode
        ORDER BY transit_mode
    """).fetchall()

    worst_routes = con.sql(f"""
        SELECT
            route_short_name,
            transit_mode,
            observations,
            on_time_rate,
            median_delay_seconds,
            p90_delay_seconds
        FROM route_summary
        WHERE observations >= {min_observations}
        ORDER BY on_time_rate ASC, p90_delay_seconds DESC
        LIMIT 15
    """).fetchall()

    worst_stops = con.sql(f"""
        SELECT
            route_short_name,
            stop_name,
            direction_id,
            observations,
            on_time_rate,
            median_delay_seconds,
            p90_delay_seconds
        FROM route_stop_summary
        WHERE observations >= {min_observations}
        ORDER BY on_time_rate ASC, p90_delay_seconds DESC
        LIMIT 15
    """).fetchall()

    day_rows = con.sql("""
        SELECT
            snapshot_date,
            observations,
            on_time_rate,
            median_delay_seconds,
            p90_delay_seconds
        FROM system_by_date
        ORDER BY on_time_rate ASC, p90_delay_seconds DESC
        LIMIT 10
    """).fetchall()

    lines = [
        "# GRT Reliability Dashboard Report",
        "",
        f"Source: `data/analysis/features/date=*/part-000.parquet`",
        f"Date range: `{overall[2]}` to `{overall[3]}`",
        "",
        "Reliability definitions:",
        "",
        "- Early: more than 60 seconds early",
        "- On-time: between 60 seconds early and 300 seconds late",
        "- Late: more than 300 seconds late",
        "",
        "## System Summary",
        "",
        markdown_table(
            [
                "Observations",
                "Days",
                "Routes",
                "Stops",
                "On-time",
                "Late",
                "Early",
                "Median delay",
                "P90 delay",
            ],
            [[
                overall[0],
                overall[1],
                overall[4],
                overall[5],
                pct(overall[9]),
                pct(overall[10]),
                pct(overall[11]),
                seconds(overall[7]),
                seconds(overall[8]),
            ]],
        ),
        "",
        "## By Mode",
        "",
        markdown_table(
            ["Mode", "Observations", "Routes", "On-time", "Median delay", "P90 delay"],
            [
                [mode, obs, routes, pct(on_time), seconds(median_delay), seconds(p90_delay)]
                for mode, obs, routes, on_time, median_delay, p90_delay in mode_rows
            ],
        ),
        "",
        f"## Least Reliable Routes (min {min_observations} observations)",
        "",
        markdown_table(
            ["Route", "Mode", "Observations", "On-time", "Median delay", "P90 delay"],
            [
                [route, mode, obs, pct(on_time), seconds(median_delay), seconds(p90_delay)]
                for route, mode, obs, on_time, median_delay, p90_delay in worst_routes
            ],
        ),
        "",
        f"## Least Reliable Route-Stops (min {min_observations} observations)",
        "",
        markdown_table(
            ["Route", "Stop", "Direction", "Observations", "On-time", "Median delay", "P90 delay"],
            [
                [route, stop, direction, obs, pct(on_time), seconds(median_delay), seconds(p90_delay)]
                for route, stop, direction, obs, on_time, median_delay, p90_delay in worst_stops
            ],
        ),
        "",
        "## Least Reliable Dates",
        "",
        markdown_table(
            ["Date", "Observations", "On-time", "Median delay", "P90 delay"],
            [
                [date, obs, pct(on_time), seconds(median_delay), seconds(p90_delay)]
                for date, obs, on_time, median_delay, p90_delay in day_rows
            ],
        ),
        "",
        "## Generated Tables",
        "",
        "- `system_by_date.csv` / `system_by_date.parquet`",
        "- `route_summary.csv` / `route_summary.parquet`",
        "- `route_by_hour.csv` / `route_by_hour.parquet`",
        "- `route_by_day_of_week.csv` / `route_by_day_of_week.parquet`",
        "- `stop_summary.csv` / `stop_summary.parquet`",
        "- `route_stop_summary.csv` / `route_stop_summary.parquet`",
        "",
    ]

    report_path = output_root / "dashboard_report.md"
    report_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"Wrote {report_path}")


def parse_args():
    parser = argparse.ArgumentParser(description="Build reliability dashboard summary tables.")
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
        help="Output directory for reliability summary tables.",
    )
    parser.add_argument(
        "--date",
        action="append",
        help="Date to include in YYYY-MM-DD form. May be repeated. Defaults to all dates.",
    )
    parser.add_argument(
        "--max-abs-delay-seconds",
        type=int,
        default=3600,
        help="Exclude observations with absolute delay above this threshold.",
    )
    parser.add_argument(
        "--min-observations",
        type=int,
        default=1000,
        help="Minimum observations for worst-route and worst-stop report sections.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    features_root = args.features_root.resolve()
    output_root = args.output_root.resolve()
    output_root.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    con.sql("SET timezone = 'UTC'")

    create_base_view(con, features_root, args.date, args.max_abs_delay_seconds)
    observations = con.sql("SELECT count(*) FROM reliability_base").fetchone()[0]
    if observations == 0:
        print("No feature rows found for reliability tables.")
        raise SystemExit(1)

    print(f"Building reliability tables from {observations} observations...")
    for table_name, query in TABLE_QUERIES.items():
        write_table(con, output_root, table_name, query)

    build_report(con, output_root, args.min_observations)


if __name__ == "__main__":
    main()
