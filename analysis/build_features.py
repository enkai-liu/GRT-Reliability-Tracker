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
DEFAULT_PARSED_ROOT = PROJECT_ROOT / "data" / "parsed"
DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "data" / "analysis" / "features"


def build_features(con, delay_root, weather_path, static_root, parsed_root, dates, snapshot_stride_minutes):
    delay_date_filter = ""
    if dates:
        date_list = ", ".join(f"'{d}'" for d in dates)
        delay_date_filter = f"AND snapshot_date IN ({date_list})"

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

    vehicle_join = ""
    vehicle_cols = """
            NULL::DOUBLE AS vehicle_lat,
            NULL::DOUBLE AS vehicle_lon,
            NULL::DOUBLE AS vehicle_bearing,
            NULL::DOUBLE AS vehicle_speed,
            NULL::BIGINT AS vehicle_current_stop_sequence,
            NULL::BIGINT AS vehicle_current_status,
            NULL::DOUBLE AS vehicle_update_age_seconds,
            NULL::BOOLEAN AS is_vehicle_update_stale,
    """

    vehicle_path = parsed_root / "vehicle_positions"
    if vehicle_path.exists():
        vehicle_join = """
        LEFT JOIN vehicle_positions vp
            ON d.snapshot_date = vp.snapshot_date
            AND d.collected_at_utc = vp.collected_at_utc
            AND d.trip_id = vp.trip_id
        """
        vehicle_cols = """
            vp.latitude AS vehicle_lat,
            vp.longitude AS vehicle_lon,
            vp.bearing AS vehicle_bearing,
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
        """

    stride_cte = """
    delay_source AS (
        SELECT *
        FROM read_parquet('{delay_root}/date=*/part-000.parquet', hive_partitioning=true)
        WHERE prediction_lead_minutes >= 0
        {delay_date_filter}
    ),

    final_target_source AS (
        SELECT * FROM delay_source
    )
    """.format(delay_root=delay_root, delay_date_filter=delay_date_filter)

    if snapshot_stride_minutes:
        stride_seconds = snapshot_stride_minutes * 60
        stride_cte = """
    delay_source_all AS (
        SELECT *
        FROM read_parquet('{delay_root}/date=*/part-000.parquet', hive_partitioning=true)
        WHERE prediction_lead_minutes >= 0
        {delay_date_filter}
    ),

    delay_source AS (
        SELECT * EXCLUDE (rn, snapshot_stride_bucket)
        FROM (
            SELECT *,
                floor(epoch(collected_at_utc) / {stride_seconds}) AS snapshot_stride_bucket,
                ROW_NUMBER() OVER (
                    PARTITION BY snapshot_date, trip_id, stop_id,
                                 floor(epoch(collected_at_utc) / {stride_seconds})
                    ORDER BY collected_at_utc DESC
                ) AS rn
            FROM delay_source_all
        )
        WHERE rn = 1
    ),

    final_target_source AS (
        SELECT * FROM delay_source_all
    )
        """.format(
            delay_root=delay_root,
            delay_date_filter=delay_date_filter,
            stride_seconds=stride_seconds,
        )

    query = f"""
    WITH {stride_cte},

    final_targets AS (
        SELECT
            snapshot_date,
            trip_id,
            stop_id,
            arg_max(delay_seconds, collected_at_utc) AS final_delay_seconds,
            arg_max(predicted_arrival_utc, collected_at_utc) AS final_predicted_arrival_utc,
            max(collected_at_utc) AS final_collected_at_utc
        FROM final_target_source
        GROUP BY snapshot_date, trip_id, stop_id
    ),

    delay_lag_features AS (
        SELECT
            d.*,
            d.delay_seconds AS current_predicted_delay_seconds,
            ft.final_delay_seconds,
            ft.final_predicted_arrival_utc,
            ft.final_collected_at_utc,
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
        JOIN final_targets ft
            ON d.snapshot_date = ft.snapshot_date
            AND d.trip_id = ft.trip_id
            AND d.stop_id = ft.stop_id
        WINDOW
            trip_stop_history AS (
                PARTITION BY d.snapshot_date, d.trip_id, d.stop_id
                ORDER BY d.collected_at_utc
            ),
            recent_trip_stop_history AS (
                PARTITION BY d.snapshot_date, d.trip_id, d.stop_id
                ORDER BY d.collected_at_utc
                ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
            ),
            trip_snapshot_stops AS (
                PARTITION BY d.snapshot_date, d.trip_id, d.collected_at_utc
                ORDER BY d.stop_sequence
            )
    ),

    vehicle_positions AS (
        SELECT
            snapshot_date,
            collected_at_utc,
            trip_id,
            any_value(vehicle_id) AS vehicle_id,
            any_value(latitude) AS latitude,
            any_value(longitude) AS longitude,
            any_value(bearing) AS bearing,
            any_value(speed) AS speed,
            any_value(current_stop_sequence) AS current_stop_sequence,
            any_value(current_status) AS current_status,
            max(vehicle_timestamp_utc) AS vehicle_timestamp_utc
        FROM read_parquet('{parsed_root}/vehicle_positions/date=*/part-000.parquet',
                          hive_partitioning=true)
        WHERE trip_id IS NOT NULL
        {delay_date_filter}
        GROUP BY snapshot_date, collected_at_utc, trip_id
    ),

    trip_max_seq AS (
        -- Compute max stop_sequence per trip for normalization
        SELECT
            trip_id,
            p_feed,
            p_snapshot_date,
            MAX(CAST(stop_sequence AS INTEGER)) AS max_stop_seq
        FROM read_parquet('{static_root}/stop_times/p_feed=*/p_snapshot_date=*/part-000.parquet',
                          hive_partitioning=true)
        GROUP BY trip_id, p_feed, p_snapshot_date
    ),

    routes AS (
        SELECT DISTINCT p_feed, p_snapshot_date, route_id, route_short_name, route_type
        FROM read_parquet('{static_root}/routes/p_feed=*/p_snapshot_date=*/part-000.parquet',
                          hive_partitioning=true)
    ),

    trips AS (
        SELECT DISTINCT
            p_feed,
            p_snapshot_date,
            trip_id,
            trip_headsign,
            service_id,
            direction_id AS static_direction_id
        FROM read_parquet('{static_root}/trips/p_feed=*/p_snapshot_date=*/part-000.parquet',
                          hive_partitioning=true)
    ),

    stops AS (
        SELECT DISTINCT p_feed, p_snapshot_date, stop_id, stop_name, stop_lat, stop_lon
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
            COALESCE(d.direction_id, TRY_CAST(t.static_direction_id AS BIGINT)) AS direction_id,
            d.vehicle_id,
            d.feed_name,

            -- Target
            d.final_delay_seconds AS delay_seconds,
            d.current_predicted_delay_seconds,
            d.final_delay_seconds - d.current_predicted_delay_seconds
                AS current_prediction_error_seconds,

            -- Time features
            d.collected_at_utc,
            d.scheduled_arrival_utc,
            d.predicted_arrival_utc,
            d.final_predicted_arrival_utc,
            d.final_collected_at_utc,
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
                THEN (
                    CASE
                        -- LRT realtime stop_sequence is one-based; static GTFS is zero-based.
                        WHEN d.feed_name LIKE 'lrt_%' THEN d.stop_sequence - 1
                        ELSE d.stop_sequence
                    END
                )::DOUBLE / tms.max_stop_seq
                ELSE 0.0
            END AS stop_sequence_normalized,

            -- Snapshot-history features
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

            -- Vehicle-position features
            {vehicle_cols}
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

            -- Weather features
            {weather_cols}

            -- Feed type
            CASE WHEN d.feed_name LIKE 'bus_%' THEN 'bus' ELSE 'lrt' END AS transit_mode

    FROM delay_lag_features d
    LEFT JOIN routes r
        ON d.route_id = r.route_id
        AND CASE WHEN d.feed_name LIKE 'bus_%' THEN 'bus_static_gtfs' ELSE 'lrt_static_gtfs' END = r.p_feed
        AND d.snapshot_date = r.p_snapshot_date
    LEFT JOIN trips t
        ON d.trip_id = t.trip_id
        AND CASE WHEN d.feed_name LIKE 'bus_%' THEN 'bus_static_gtfs' ELSE 'lrt_static_gtfs' END = t.p_feed
        AND d.snapshot_date = t.p_snapshot_date
    LEFT JOIN stops s
        ON d.stop_id = s.stop_id
        AND CASE WHEN d.feed_name LIKE 'bus_%' THEN 'bus_static_gtfs' ELSE 'lrt_static_gtfs' END = s.p_feed
        AND d.snapshot_date = s.p_snapshot_date
    LEFT JOIN trip_max_seq tms
        ON d.trip_id = tms.trip_id
        AND CASE WHEN d.feed_name LIKE 'bus_%' THEN 'bus_static_gtfs' ELSE 'lrt_static_gtfs' END = tms.p_feed
        AND d.snapshot_date = tms.p_snapshot_date
    {vehicle_join}
    {weather_join}
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


def discover_dates(delay_root):
    dates = []
    if not delay_root.exists():
        return dates
    for entry in sorted(delay_root.iterdir()):
        if entry.is_dir() and entry.name.startswith("date="):
            dates.append(entry.name.removeprefix("date="))
    return dates


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
        "--parsed-root", type=Path, default=DEFAULT_PARSED_ROOT,
        help="Root of parsed realtime Parquet tables, used for vehicle-position features.",
    )
    parser.add_argument(
        "--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT,
        help="Output directory for feature table Parquet.",
    )
    parser.add_argument(
        "--snapshot-stride-minutes", type=int,
        help=(
            "For all-snapshot delay tables, keep at most one row per trip-stop "
            "per N-minute collection bucket. Defaults to keeping every row."
        ),
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
    parsed_root = args.parsed_root.resolve()
    output_root = args.output_root.resolve()

    dates = args.date if args.date else discover_dates(delay_root)
    if not dates:
        print(f"No dates found under {delay_root}")
        raise SystemExit(1)

    print(f"Building enriched feature table for {len(dates)} dates...")
    total_rows = 0
    for snapshot_date in dates:
        con = duckdb.connect()
        con.sql("SET timezone = 'UTC'")
        con.sql("SET preserve_insertion_order = false")

        result = build_features(
            con,
            delay_root,
            weather_path,
            static_root,
            parsed_root,
            [snapshot_date],
            args.snapshot_stride_minutes,
        )
        con.sql("CREATE TABLE result AS SELECT * FROM result")

        row_count = con.sql("SELECT count(*) FROM result").fetchone()[0]
        total_rows += row_count
        print(f"  {snapshot_date}: built {row_count} feature rows")

        write_output(con, "result", output_root, [snapshot_date], args.overwrite)
        con.close()

    print(f"\nDone. Total: {total_rows} rows across {len(dates)} dates.")


if __name__ == "__main__":
    main()
