import argparse
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from dotenv import load_dotenv
from google.cloud import storage


PROJECT_ROOT = Path(__file__).resolve().parents[1]
REALTIME_FEEDS = (
    "bus_trip_updates",
    "bus_vehicle_positions",
    "lrt_trip_updates",
    "lrt_vehicle_positions",
)
ALERT_FEEDS = (
    "grt_service_alerts",
)
STATIC_FEEDS = (
    "bus_static_gtfs",
    "lrt_static_gtfs",
)
WEATHER_FORECAST_LOCATIONS = (
    "kitchener_waterloo",
)


def utc_now():
    return datetime.now(timezone.utc)


def today_prefix(now):
    return now.strftime("%Y-%m-%d")


def latest_blob(bucket, prefix):
    blobs = bucket.list_blobs(prefix=prefix)
    return max(blobs, key=lambda blob: blob.updated, default=None)


def check_realtime_feed(bucket, feed_name, now, max_age_minutes):
    prefix = f"raw/{feed_name}/{today_prefix(now)}/"
    blob = latest_blob(bucket, prefix)

    if blob is None:
        return False, f"{feed_name}: no snapshots found under gs://{bucket.name}/{prefix}"

    age = now - blob.updated
    if age > timedelta(minutes=max_age_minutes):
        return (
            False,
            f"{feed_name}: latest snapshot is stale "
            f"({age.total_seconds() / 60:.1f} min old): gs://{bucket.name}/{blob.name}",
        )

    return (
        True,
        f"{feed_name}: ok ({age.total_seconds() / 60:.1f} min old): gs://{bucket.name}/{blob.name}",
    )


def check_static_feed(bucket, feed_name, now, max_age_hours):
    prefix = f"static_gtfs/{feed_name}/"
    blob = latest_blob(bucket, prefix)

    if blob is None:
        return False, f"{feed_name}: no static GTFS snapshots found under gs://{bucket.name}/{prefix}"

    age = now - blob.updated
    if age > timedelta(hours=max_age_hours):
        return (
            False,
            f"{feed_name}: latest GTFS ZIP is stale "
            f"({age.total_seconds() / 3600:.1f} hr old): gs://{bucket.name}/{blob.name}",
        )

    return (
        True,
        f"{feed_name}: ok ({age.total_seconds() / 3600:.1f} hr old): gs://{bucket.name}/{blob.name}",
    )


def check_weather_forecast(bucket, location_name, now, max_age_hours):
    prefix = f"raw/weather_forecasts/{location_name}/"
    blob = latest_blob(bucket, prefix)

    if blob is None:
        return False, f"{location_name}: no weather forecast snapshots found under gs://{bucket.name}/{prefix}"

    age = now - blob.updated
    if age > timedelta(hours=max_age_hours):
        return (
            False,
            f"{location_name}: latest weather forecast is stale "
            f"({age.total_seconds() / 3600:.1f} hr old): gs://{bucket.name}/{blob.name}",
        )

    return (
        True,
        f"{location_name}: ok ({age.total_seconds() / 3600:.1f} hr old): gs://{bucket.name}/{blob.name}",
    )


def parse_args():
    parser = argparse.ArgumentParser(description="Check whether recent GRT collector snapshots exist in GCS.")
    parser.add_argument(
        "--bucket",
        default=os.getenv("GCS_BUCKET"),
        help="GCS bucket name. Defaults to GCS_BUCKET from .env.",
    )
    parser.add_argument(
        "--max-realtime-age-minutes",
        type=int,
        default=5,
        help="Maximum acceptable age for realtime snapshots.",
    )
    parser.add_argument(
        "--max-alert-age-minutes",
        type=int,
        default=15,
        help="Maximum acceptable age for service alert snapshots.",
    )
    parser.add_argument(
        "--max-static-age-hours",
        type=int,
        default=48,
        help="Maximum acceptable age for static GTFS ZIP snapshots.",
    )
    parser.add_argument(
        "--max-weather-forecast-age-hours",
        type=int,
        default=6,
        help="Maximum acceptable age for weather forecast snapshots.",
    )
    parser.add_argument(
        "--skip-static",
        action="store_true",
        help="Only check realtime feeds.",
    )
    parser.add_argument(
        "--skip-weather",
        action="store_true",
        help="Skip weather forecast checks.",
    )
    return parser.parse_args()


def main():
    load_dotenv(PROJECT_ROOT / ".env")
    args = parse_args()

    if not args.bucket:
        print("GCS bucket is required. Set GCS_BUCKET in .env or pass --bucket.")
        raise SystemExit(2)

    bucket = storage.Client().bucket(args.bucket)
    now = utc_now()
    results = []

    for feed_name in REALTIME_FEEDS:
        results.append(check_realtime_feed(bucket, feed_name, now, args.max_realtime_age_minutes))

    for feed_name in ALERT_FEEDS:
        results.append(check_realtime_feed(bucket, feed_name, now, args.max_alert_age_minutes))

    if not args.skip_static:
        for feed_name in STATIC_FEEDS:
            results.append(check_static_feed(bucket, feed_name, now, args.max_static_age_hours))

    if not args.skip_weather:
        for location_name in WEATHER_FORECAST_LOCATIONS:
            results.append(check_weather_forecast(bucket, location_name, now, args.max_weather_forecast_age_hours))

    for ok, message in results:
        status = "OK" if ok else "FAIL"
        print(f"[{status}] {message}")

    raise SystemExit(0 if all(ok for ok, _ in results) else 1)


if __name__ == "__main__":
    main()
