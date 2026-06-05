"""Build enriched feature table by joining delay data with weather and static GTFS metadata.

Joins:
  - delay_table (from build_delay_table.py)
  - weather_features (from build_weather_features.py)
  - static GTFS: routes, trips, stops

Derives time-based features (hour, day_of_week, is_rush_hour, etc.) and normalizes
stop_sequence within each trip.

Output: data/analysis/features/date=YYYY-MM-DD/part-000.parquet
"""

import argparse
from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DELAY_ROOT = PROJECT_ROOT / "data" / "analysis" / "delay_table"
DEFAULT_WEATHER_PATH = PROJECT_ROOT / "data" / "analysis" / "weather_features" / "part-000.parquet"
DEFAULT_STATIC_ROOT = PROJECT_ROOT / "data" / "parsed_static_gtfs"
DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "data" / "analysis" / "features"


def build_features(con, delay_root, weather_path, static_root, dates):
    date_filter = ""
    if dates:
        date_list = ", ".join(f"'{d}'" for d in dates)
        date_filter = f"WHERE d.snapshot_date IN ({date_list})"

    weather_join = ""
    weather_cols = """
            NULL::BIGINT AS temperature_c,
            NULL::VARCHAR AS condition,
            NULL::BIGINT AS wind_speed_kmh,
            NULL::BIGINT AS wind_gust_kmh,
            NULL::BIGINT AS precip_probability_pct,
            NULL::VARCHAR AS precip_category,
            NULL::BOOLEAN AS is_rain,
            NULL::BOOLEAN AS is_snow,
            NULL::BOOLEAN AS is_freezing_rain,
            NULL::BOOLEAN AS is_precip,
    """

    if weather_path.exists():
        weather_join = f"""
        LEFT JOIN read_parquet('{weather_path}') w
            ON date_trunc('hour', d.scheduled_arrival_utc) = w.timestamp_utc_hour
            AND w.location = 'kitchener_waterloo'
        """
        weather_cols = """
            w.temperature_c,
            w.condition AS weather_condition,
            w.wind_speed_kmh,
            w.wind_gust_kmh,
            w.precip_probability_pct,
            w.precip_category,
            w.is_rain,
            w.is_snow,
            w.is_freezing_rain,
            w.is_precip,
        """

    query = f"""
    WITH trip_max_seq AS (
        -- Compute max stop_sequence per trip for normalization
        SELECT trip_id, p_feed, MAX(CAST(stop_sequence AS INTEGER)) AS max_stop_seq
        FROM read_parquet('{static_root}/stop_times/p_feed=*/p_snapshot_date=*/part-000.parquet',
                          hive_partitioning=true)
        GROUP BY trip_id, p_feed
    ),

    routes AS (
        SELECT DISTINCT route_id, route_short_name, route_type
        FROM read_parquet('{static_root}/routes/p_feed=*/p_snapshot_date=*/part-000.parquet',
                          hive_partitioning=true)
    ),

    trips AS (
        SELECT DISTINCT trip_id, trip_headsign, service_id, direction_id AS static_direction_id
        FROM read_parquet('{static_root}/trips/p_feed=*/p_snapshot_date=*/part-000.parquet',
                          hive_partitioning=true)
    ),

    stops AS (
        SELECT DISTINCT stop_id, stop_name, stop_lat, stop_lon
        FROM read_parquet('{static_root}/stops/p_feed=*/p_snapshot_date=*/part-000.parquet',
                          hive_partitioning=true)
    )

    SELECT
            -- Identifiers
            d.snapshot_date,
            d.trip_id,
            d.route_id,
            d.stop_id,
            d.stop_sequence,
            d.direction_id,
            d.feed_name,

            -- Target
            d.delay_seconds,

            -- Time features
            d.scheduled_arrival_utc,
            d.predicted_arrival_utc,
            d.prediction_lead_minutes,
            d.service_date,
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

            -- Route metadata
            r.route_short_name,
            CAST(r.route_type AS INTEGER) AS route_type,

            -- Trip metadata
            t.trip_headsign,

            -- Stop metadata
            s.stop_name,
            CAST(s.stop_lat AS DOUBLE) AS stop_lat,
            CAST(s.stop_lon AS DOUBLE) AS stop_lon,

            -- Stop sequence normalized (0 to 1 within trip)
            CASE WHEN tms.max_stop_seq > 0
                THEN d.stop_sequence::DOUBLE / tms.max_stop_seq
                ELSE 0.0
            END AS stop_sequence_normalized,

            -- Weather features
            {weather_cols}

            -- Feed type
            CASE WHEN d.feed_name LIKE 'bus_%' THEN 'bus' ELSE 'lrt' END AS transit_mode

    FROM read_parquet('{delay_root}/date=*/part-000.parquet', hive_partitioning=true) d
    LEFT JOIN routes r ON d.route_id = r.route_id
    LEFT JOIN trips t ON d.trip_id = t.trip_id
    LEFT JOIN stops s ON d.stop_id = s.stop_id
    LEFT JOIN trip_max_seq tms
        ON d.trip_id = tms.trip_id
        AND CASE WHEN d.feed_name LIKE 'bus_%' THEN 'bus_static_gtfs' ELSE 'lrt_static_gtfs' END = tms.p_feed
    {weather_join}
    {date_filter}
    ORDER BY d.snapshot_date, d.trip_id, d.stop_sequence
    """

    return con.sql(query)


def write_output(con, result, output_root, dates, overwrite):
    if dates:
        date_list = dates
    else:
        date_list = [
            row[0] for row in con.sql(
                "SELECT DISTINCT snapshot_date FROM result ORDER BY snapshot_date"
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
        print(f"Wrote {row_count} rows to {output_path}")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Build enriched feature table from delay data, weather, and static GTFS."
    )
    parser.add_argument(
        "--delay-root", type=Path, default=DEFAULT_DELAY_ROOT,
        help="Root directory of delay table Parquet files.",
    )
    parser.add_argument(
        "--weather-path", type=Path, default=DEFAULT_WEATHER_PATH,
        help="Path to weather features Parquet file.",
    )
    parser.add_argument(
        "--static-root", type=Path, default=DEFAULT_STATIC_ROOT,
        help="Root of parsed static GTFS Parquet tables.",
    )
    parser.add_argument(
        "--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT,
        help="Output directory for feature table Parquet.",
    )
    parser.add_argument(
        "--date", action="append",
        help="Date to process in YYYY-MM-DD form. May be repeated. Defaults to all.",
    )
    parser.add_argument("--overwrite", action="store_true", help="Replace existing outputs.")
    return parser.parse_args()


def main():
    args = parse_args()
    delay_root = args.delay_root.resolve()
    weather_path = args.weather_path.resolve()
    static_root = args.static_root.resolve()
    output_root = args.output_root.resolve()

    con = duckdb.connect()
    con.sql("SET timezone = 'UTC'")

    print("Building enriched feature table...")
    result = build_features(con, delay_root, weather_path, static_root, args.date)
    con.sql("CREATE TABLE result AS SELECT * FROM result")

    total = con.sql("SELECT count(*) FROM result").fetchone()[0]
    print(f"Built {total} feature rows")

    write_output(con, "result", output_root, args.date, args.overwrite)

    # Summary
    con.sql("""
        SELECT
            count(*) AS rows,
            count(DISTINCT route_short_name) AS routes,
            count(*) FILTER (WHERE temperature_c IS NOT NULL) AS with_weather,
            avg(delay_seconds)::INTEGER AS avg_delay_s,
            count(*) FILTER (WHERE is_rush_hour) AS rush_hour_rows,
            count(*) FILTER (WHERE is_weekend) AS weekend_rows
        FROM result
    """).show()


if __name__ == "__main__":
    main()
