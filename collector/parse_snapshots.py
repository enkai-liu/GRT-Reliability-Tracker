import argparse
import os
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv
from google.cloud import storage
from google.transit import gtfs_realtime_pb2


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RAW_ROOT = PROJECT_ROOT / "data" / "raw"
DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "data" / "parsed"
REALTIME_FEEDS = (
    "bus_trip_updates",
    "bus_vehicle_positions",
    "lrt_trip_updates",
    "lrt_vehicle_positions",
    "grt_service_alerts",
)

SNAPSHOT_SCHEMA = pa.schema(
    [
        ("feed_name", pa.string()),
        ("snapshot_path", pa.string()),
        ("snapshot_date", pa.string()),
        ("collected_at_utc", pa.timestamp("s", tz="UTC")),
        ("feed_timestamp_utc", pa.timestamp("s", tz="UTC")),
        ("entity_count", pa.int64()),
        ("vehicle_entity_count", pa.int64()),
        ("trip_update_entity_count", pa.int64()),
        ("alert_entity_count", pa.int64()),
        ("byte_size", pa.int64()),
    ]
)

VEHICLE_POSITION_SCHEMA = pa.schema(
    [
        ("feed_name", pa.string()),
        ("snapshot_path", pa.string()),
        ("snapshot_date", pa.string()),
        ("collected_at_utc", pa.timestamp("s", tz="UTC")),
        ("feed_timestamp_utc", pa.timestamp("s", tz="UTC")),
        ("entity_id", pa.string()),
        ("trip_id", pa.string()),
        ("route_id", pa.string()),
        ("direction_id", pa.int64()),
        ("start_time", pa.string()),
        ("start_date", pa.string()),
        ("schedule_relationship", pa.int64()),
        ("vehicle_id", pa.string()),
        ("vehicle_label", pa.string()),
        ("license_plate", pa.string()),
        ("latitude", pa.float64()),
        ("longitude", pa.float64()),
        ("bearing", pa.float64()),
        ("odometer", pa.float64()),
        ("speed", pa.float64()),
        ("current_stop_sequence", pa.int64()),
        ("stop_id", pa.string()),
        ("current_status", pa.int64()),
        ("vehicle_timestamp_utc", pa.timestamp("s", tz="UTC")),
        ("congestion_level", pa.int64()),
        ("occupancy_status", pa.int64()),
    ]
)

TRIP_UPDATE_SCHEMA = pa.schema(
    [
        ("feed_name", pa.string()),
        ("snapshot_path", pa.string()),
        ("snapshot_date", pa.string()),
        ("collected_at_utc", pa.timestamp("s", tz="UTC")),
        ("feed_timestamp_utc", pa.timestamp("s", tz="UTC")),
        ("entity_id", pa.string()),
        ("trip_id", pa.string()),
        ("route_id", pa.string()),
        ("direction_id", pa.int64()),
        ("start_time", pa.string()),
        ("start_date", pa.string()),
        ("schedule_relationship", pa.int64()),
        ("vehicle_id", pa.string()),
        ("vehicle_label", pa.string()),
        ("license_plate", pa.string()),
        ("trip_update_timestamp_utc", pa.timestamp("s", tz="UTC")),
        ("trip_delay_seconds", pa.int64()),
        ("stop_time_update_count", pa.int64()),
    ]
)

STOP_TIME_UPDATE_SCHEMA = pa.schema(
    [
        ("feed_name", pa.string()),
        ("snapshot_path", pa.string()),
        ("snapshot_date", pa.string()),
        ("collected_at_utc", pa.timestamp("s", tz="UTC")),
        ("feed_timestamp_utc", pa.timestamp("s", tz="UTC")),
        ("entity_id", pa.string()),
        ("trip_id", pa.string()),
        ("route_id", pa.string()),
        ("direction_id", pa.int64()),
        ("start_time", pa.string()),
        ("start_date", pa.string()),
        ("trip_schedule_relationship", pa.int64()),
        ("vehicle_id", pa.string()),
        ("stop_update_index", pa.int64()),
        ("stop_sequence", pa.int64()),
        ("stop_id", pa.string()),
        ("stop_schedule_relationship", pa.int64()),
        ("arrival_delay_seconds", pa.int64()),
        ("arrival_time_utc", pa.timestamp("s", tz="UTC")),
        ("arrival_uncertainty_seconds", pa.int64()),
        ("departure_delay_seconds", pa.int64()),
        ("departure_time_utc", pa.timestamp("s", tz="UTC")),
        ("departure_uncertainty_seconds", pa.int64()),
    ]
)

SERVICE_ALERT_SCHEMA = pa.schema(
    [
        ("feed_name", pa.string()),
        ("snapshot_path", pa.string()),
        ("snapshot_date", pa.string()),
        ("collected_at_utc", pa.timestamp("s", tz="UTC")),
        ("feed_timestamp_utc", pa.timestamp("s", tz="UTC")),
        ("entity_id", pa.string()),
        ("cause", pa.int64()),
        ("effect", pa.int64()),
        ("severity_level", pa.int64()),
        ("header_text", pa.string()),
        ("description_text", pa.string()),
        ("url", pa.string()),
        ("tts_header_text", pa.string()),
        ("tts_description_text", pa.string()),
        ("active_period_count", pa.int64()),
        ("informed_entity_count", pa.int64()),
    ]
)

SERVICE_ALERT_ACTIVE_PERIOD_SCHEMA = pa.schema(
    [
        ("feed_name", pa.string()),
        ("snapshot_path", pa.string()),
        ("snapshot_date", pa.string()),
        ("collected_at_utc", pa.timestamp("s", tz="UTC")),
        ("feed_timestamp_utc", pa.timestamp("s", tz="UTC")),
        ("entity_id", pa.string()),
        ("active_period_index", pa.int64()),
        ("start_time_utc", pa.timestamp("s", tz="UTC")),
        ("end_time_utc", pa.timestamp("s", tz="UTC")),
    ]
)

SERVICE_ALERT_INFORMED_ENTITY_SCHEMA = pa.schema(
    [
        ("feed_name", pa.string()),
        ("snapshot_path", pa.string()),
        ("snapshot_date", pa.string()),
        ("collected_at_utc", pa.timestamp("s", tz="UTC")),
        ("feed_timestamp_utc", pa.timestamp("s", tz="UTC")),
        ("entity_id", pa.string()),
        ("informed_entity_index", pa.int64()),
        ("agency_id", pa.string()),
        ("route_id", pa.string()),
        ("route_type", pa.int64()),
        ("stop_id", pa.string()),
        ("trip_id", pa.string()),
        ("direction_id", pa.int64()),
        ("start_time", pa.string()),
        ("start_date", pa.string()),
        ("trip_schedule_relationship", pa.int64()),
    ]
)

TABLE_SCHEMAS = {
    "feed_snapshots": SNAPSHOT_SCHEMA,
    "vehicle_positions": VEHICLE_POSITION_SCHEMA,
    "trip_updates": TRIP_UPDATE_SCHEMA,
    "stop_time_updates": STOP_TIME_UPDATE_SCHEMA,
    "service_alerts": SERVICE_ALERT_SCHEMA,
    "service_alert_active_periods": SERVICE_ALERT_ACTIVE_PERIOD_SCHEMA,
    "service_alert_informed_entities": SERVICE_ALERT_INFORMED_ENTITY_SCHEMA,
}


def timestamp_to_datetime(timestamp):
    if not timestamp:
        return None
    return datetime.fromtimestamp(timestamp, tz=timezone.utc)


def parse_collected_at(path):
    stem = path.stem
    try:
        return datetime.strptime(stem, "%Y-%m-%dT%H-%M-%SZ").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def optional_scalar(message, field_name):
    try:
        if message.HasField(field_name):
            return getattr(message, field_name)
    except ValueError:
        return getattr(message, field_name, None)
    return None


def optional_timestamp(message, field_name):
    value = optional_scalar(message, field_name)
    return timestamp_to_datetime(value)


def trip_fields(trip):
    return {
        "trip_id": trip.trip_id or None,
        "route_id": trip.route_id or None,
        "direction_id": optional_scalar(trip, "direction_id"),
        "start_time": trip.start_time or None,
        "start_date": trip.start_date or None,
        "schedule_relationship": optional_scalar(trip, "schedule_relationship"),
    }


def vehicle_descriptor_fields(vehicle):
    return {
        "vehicle_id": vehicle.id or None,
        "vehicle_label": vehicle.label or None,
        "license_plate": vehicle.license_plate or None,
    }


def translated_text(translated_string):
    if not translated_string.translation:
        return None

    english = [
        translation.text
        for translation in translated_string.translation
        if translation.text and translation.language.lower().startswith("en")
    ]
    if english:
        return english[0]

    return translated_string.translation[0].text or None


def parse_snapshot(path, feed_name):
    content = path.read_bytes()
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(content)

    collected_at = parse_collected_at(path)
    snapshot_date = path.parent.name
    snapshot_path = str(path)
    feed_timestamp = timestamp_to_datetime(feed.header.timestamp)

    rows = {
        "feed_snapshots": [],
        "vehicle_positions": [],
        "trip_updates": [],
        "stop_time_updates": [],
        "service_alerts": [],
        "service_alert_active_periods": [],
        "service_alert_informed_entities": [],
    }
    counts = {
        "vehicle": 0,
        "trip_update": 0,
        "alert": 0,
    }

    for entity in feed.entity:
        if entity.HasField("vehicle"):
            counts["vehicle"] += 1
            vehicle = entity.vehicle
            trip = trip_fields(vehicle.trip)
            vehicle_descriptor = vehicle_descriptor_fields(vehicle.vehicle)

            position = vehicle.position if vehicle.HasField("position") else None
            rows["vehicle_positions"].append(
                {
                    "feed_name": feed_name,
                    "snapshot_path": snapshot_path,
                    "snapshot_date": snapshot_date,
                    "collected_at_utc": collected_at,
                    "feed_timestamp_utc": feed_timestamp,
                    "entity_id": entity.id or None,
                    **trip,
                    **vehicle_descriptor,
                    "latitude": position.latitude if position else None,
                    "longitude": position.longitude if position else None,
                    "bearing": optional_scalar(position, "bearing") if position else None,
                    "odometer": optional_scalar(position, "odometer") if position else None,
                    "speed": optional_scalar(position, "speed") if position else None,
                    "current_stop_sequence": optional_scalar(vehicle, "current_stop_sequence"),
                    "stop_id": vehicle.stop_id or None,
                    "current_status": optional_scalar(vehicle, "current_status"),
                    "vehicle_timestamp_utc": optional_timestamp(vehicle, "timestamp"),
                    "congestion_level": optional_scalar(vehicle, "congestion_level"),
                    "occupancy_status": optional_scalar(vehicle, "occupancy_status"),
                }
            )

        elif entity.HasField("trip_update"):
            counts["trip_update"] += 1
            trip_update = entity.trip_update
            trip = trip_fields(trip_update.trip)
            vehicle_descriptor = vehicle_descriptor_fields(trip_update.vehicle)

            rows["trip_updates"].append(
                {
                    "feed_name": feed_name,
                    "snapshot_path": snapshot_path,
                    "snapshot_date": snapshot_date,
                    "collected_at_utc": collected_at,
                    "feed_timestamp_utc": feed_timestamp,
                    "entity_id": entity.id or None,
                    **trip,
                    **vehicle_descriptor,
                    "trip_update_timestamp_utc": optional_timestamp(trip_update, "timestamp"),
                    "trip_delay_seconds": optional_scalar(trip_update, "delay"),
                    "stop_time_update_count": len(trip_update.stop_time_update),
                }
            )

            for index, stop_update in enumerate(trip_update.stop_time_update):
                rows["stop_time_updates"].append(
                    {
                        "feed_name": feed_name,
                        "snapshot_path": snapshot_path,
                        "snapshot_date": snapshot_date,
                        "collected_at_utc": collected_at,
                        "feed_timestamp_utc": feed_timestamp,
                        "entity_id": entity.id or None,
                        "trip_id": trip["trip_id"],
                        "route_id": trip["route_id"],
                        "direction_id": trip["direction_id"],
                        "start_time": trip["start_time"],
                        "start_date": trip["start_date"],
                        "trip_schedule_relationship": trip["schedule_relationship"],
                        "vehicle_id": vehicle_descriptor["vehicle_id"],
                        "stop_update_index": index,
                        "stop_sequence": optional_scalar(stop_update, "stop_sequence"),
                        "stop_id": stop_update.stop_id or None,
                        "stop_schedule_relationship": optional_scalar(stop_update, "schedule_relationship"),
                        "arrival_delay_seconds": optional_scalar(stop_update.arrival, "delay"),
                        "arrival_time_utc": optional_timestamp(stop_update.arrival, "time"),
                        "arrival_uncertainty_seconds": optional_scalar(stop_update.arrival, "uncertainty"),
                        "departure_delay_seconds": optional_scalar(stop_update.departure, "delay"),
                        "departure_time_utc": optional_timestamp(stop_update.departure, "time"),
                        "departure_uncertainty_seconds": optional_scalar(stop_update.departure, "uncertainty"),
                    }
                )

        elif entity.HasField("alert"):
            counts["alert"] += 1
            alert = entity.alert

            rows["service_alerts"].append(
                {
                    "feed_name": feed_name,
                    "snapshot_path": snapshot_path,
                    "snapshot_date": snapshot_date,
                    "collected_at_utc": collected_at,
                    "feed_timestamp_utc": feed_timestamp,
                    "entity_id": entity.id or None,
                    "cause": optional_scalar(alert, "cause"),
                    "effect": optional_scalar(alert, "effect"),
                    "severity_level": optional_scalar(alert, "severity_level"),
                    "header_text": translated_text(alert.header_text),
                    "description_text": translated_text(alert.description_text),
                    "url": translated_text(alert.url),
                    "tts_header_text": translated_text(alert.tts_header_text),
                    "tts_description_text": translated_text(alert.tts_description_text),
                    "active_period_count": len(alert.active_period),
                    "informed_entity_count": len(alert.informed_entity),
                }
            )

            for index, active_period in enumerate(alert.active_period):
                rows["service_alert_active_periods"].append(
                    {
                        "feed_name": feed_name,
                        "snapshot_path": snapshot_path,
                        "snapshot_date": snapshot_date,
                        "collected_at_utc": collected_at,
                        "feed_timestamp_utc": feed_timestamp,
                        "entity_id": entity.id or None,
                        "active_period_index": index,
                        "start_time_utc": optional_timestamp(active_period, "start"),
                        "end_time_utc": optional_timestamp(active_period, "end"),
                    }
                )

            for index, informed_entity in enumerate(alert.informed_entity):
                trip = trip_fields(informed_entity.trip)
                rows["service_alert_informed_entities"].append(
                    {
                        "feed_name": feed_name,
                        "snapshot_path": snapshot_path,
                        "snapshot_date": snapshot_date,
                        "collected_at_utc": collected_at,
                        "feed_timestamp_utc": feed_timestamp,
                        "entity_id": entity.id or None,
                        "informed_entity_index": index,
                        "agency_id": informed_entity.agency_id or None,
                        "route_id": informed_entity.route_id or None,
                        "route_type": optional_scalar(informed_entity, "route_type"),
                        "stop_id": informed_entity.stop_id or None,
                        "trip_id": trip["trip_id"],
                        "direction_id": trip["direction_id"],
                        "start_time": trip["start_time"],
                        "start_date": trip["start_date"],
                        "trip_schedule_relationship": trip["schedule_relationship"],
                    }
                )

    rows["feed_snapshots"].append(
        {
            "feed_name": feed_name,
            "snapshot_path": snapshot_path,
            "snapshot_date": snapshot_date,
            "collected_at_utc": collected_at,
            "feed_timestamp_utc": feed_timestamp,
            "entity_count": len(feed.entity),
            "vehicle_entity_count": counts["vehicle"],
            "trip_update_entity_count": counts["trip_update"],
            "alert_entity_count": counts["alert"],
            "byte_size": len(content),
        }
    )

    return rows


def find_snapshot_paths(raw_root, dates, feeds):
    selected_feeds = feeds or REALTIME_FEEDS
    selected_dates = set(dates) if dates else None

    for feed_name in selected_feeds:
        feed_dir = raw_root / feed_name
        if not feed_dir.exists():
            continue

        for date_dir in sorted(item for item in feed_dir.iterdir() if item.is_dir()):
            if selected_dates and date_dir.name not in selected_dates:
                continue

            for path in sorted(date_dir.glob("*.pb")):
                yield feed_name, path


def sync_raw_snapshots_from_gcs(bucket_name, raw_root, dates, feeds):
    if not bucket_name:
        raise ValueError("GCS bucket is required for --sync-from-gcs")
    if not dates:
        raise ValueError("--date is required with --sync-from-gcs to avoid downloading the whole bucket")

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    selected_feeds = feeds or REALTIME_FEEDS
    downloaded = 0

    for feed_name in selected_feeds:
        for snapshot_date in dates:
            prefix = f"raw/{feed_name}/{snapshot_date}/"
            for blob in bucket.list_blobs(prefix=prefix):
                if not blob.name.endswith(".pb"):
                    continue

                target_path = raw_root / blob.name.removeprefix("raw/")
                target_path.parent.mkdir(parents=True, exist_ok=True)
                blob.download_to_filename(target_path)
                downloaded += 1

    print(f"Downloaded {downloaded} raw snapshots from gs://{bucket_name}")


def upload_parquet_to_gcs(bucket_name, output_root, output_paths):
    if not bucket_name:
        raise ValueError("GCS bucket is required for --upload-to-gcs")

    client = storage.Client()
    bucket = client.bucket(bucket_name)

    for path in output_paths:
        object_name = str(path.relative_to(output_root))
        blob = bucket.blob(f"parsed/{object_name}")
        blob.upload_from_filename(path, content_type="application/vnd.apache.parquet")
        print(f"Uploaded {path} to gs://{bucket_name}/parsed/{object_name}")


def write_table(output_root, table_name, snapshot_date, rows, overwrite):
    table_dir = output_root / table_name / f"date={snapshot_date}"
    table_dir.mkdir(parents=True, exist_ok=True)

    output_path = table_dir / "part-000.parquet"
    if output_path.exists() and not overwrite:
        raise FileExistsError(f"{output_path} already exists. Pass --overwrite to replace it.")

    table = pa.Table.from_pylist(rows, schema=TABLE_SCHEMAS[table_name])
    pq.write_table(table, output_path, compression="snappy")
    return output_path


def positive_int(value):
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be greater than zero")
    return parsed


def parse_args():
    parser = argparse.ArgumentParser(description="Parse raw GTFS-Realtime .pb snapshots into Parquet tables.")
    parser.add_argument("--raw-root", type=Path, default=DEFAULT_RAW_ROOT, help="Raw snapshot root directory.")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT, help="Parsed Parquet output root.")
    parser.add_argument("--gcs-bucket", default=None, help="GCS bucket for optional sync/upload operations.")
    parser.add_argument(
        "--sync-from-gcs",
        action="store_true",
        help="Download matching raw snapshots from GCS before parsing. Requires --date.",
    )
    parser.add_argument("--upload-to-gcs", action="store_true", help="Upload written Parquet files to GCS.")
    parser.add_argument("--date", action="append", help="Date to parse in YYYY-MM-DD form. May be repeated.")
    parser.add_argument(
        "--feed",
        action="append",
        choices=REALTIME_FEEDS,
        help="Feed to parse. May be repeated. Defaults to all realtime feeds.",
    )
    parser.add_argument("--limit-files", type=positive_int, help="Parse at most this many snapshot files.")
    parser.add_argument("--overwrite", action="store_true", help="Replace existing Parquet outputs.")
    return parser.parse_args()


def main():
    load_dotenv(PROJECT_ROOT / ".env")
    args = parse_args()
    raw_root = args.raw_root.resolve()
    output_root = args.output_root.resolve()
    gcs_bucket = args.gcs_bucket or os.getenv("GCS_BUCKET")
    grouped_rows = defaultdict(lambda: defaultdict(list))
    parsed_files = 0
    output_paths = []

    if args.sync_from_gcs:
        sync_raw_snapshots_from_gcs(gcs_bucket, raw_root, args.date, args.feed)

    for feed_name, path in find_snapshot_paths(raw_root, args.date, args.feed):
        snapshot_rows = parse_snapshot(path, feed_name)
        snapshot_date = path.parent.name

        for table_name, rows in snapshot_rows.items():
            grouped_rows[table_name][snapshot_date].extend(rows)

        parsed_files += 1
        if args.limit_files and parsed_files >= args.limit_files:
            break

    if parsed_files == 0:
        print(f"No snapshot files found under {raw_root}")
        raise SystemExit(1)

    print(f"Parsed {parsed_files} snapshot files from {raw_root}")

    for table_name in TABLE_SCHEMAS:
        for snapshot_date, rows in sorted(grouped_rows[table_name].items()):
            output_path = write_table(output_root, table_name, snapshot_date, rows, args.overwrite)
            output_paths.append(output_path)
            print(f"Wrote {len(rows)} rows to {output_path}")

    if args.upload_to_gcs:
        upload_parquet_to_gcs(gcs_bucket, output_root, output_paths)


if __name__ == "__main__":
    main()
