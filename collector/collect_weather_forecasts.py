import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv
from google.cloud import storage


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DATA_ROOT = PROJECT_ROOT / "data"
DEFAULT_LOCATIONS = (
    ("kitchener_waterloo", "on-82"),
    ("cambridge", "on-81"),
)
DEFAULT_API_URL = "https://api.weather.gc.ca/collections/citypageweather-realtime/items"
REQUEST_TIMEOUT_SECONDS = 20


def utc_now():
    return datetime.now(timezone.utc)


def timestamp_name(now):
    return now.strftime("%Y-%m-%dT%H-%M-%SZ")


def date_part(now):
    return now.strftime("%Y-%m-%d")


def make_gcs_bucket(bucket_name):
    if not bucket_name:
        return None

    client = storage.Client()
    return client.bucket(bucket_name)


def weather_feature_url(api_url, location_id):
    return f"{api_url.rstrip('/')}/{location_id}"


def fetch_forecast(location_id, api_url):
    response = requests.get(
        weather_feature_url(api_url, location_id),
        params={"f": "json"},
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    return response.json()


def validate_forecast(payload):
    properties = payload.get("properties") or {}
    hourly_group = properties.get("hourlyForecastGroup") or {}
    hourly_forecasts = hourly_group.get("hourlyForecasts") or []

    if not hourly_forecasts:
        raise ValueError("forecast payload did not include hourlyForecastGroup.hourlyForecasts")

    return len(hourly_forecasts), properties.get("lastUpdated")


def save_forecast(data_root, location_name, payload, now):
    object_name = f"raw/weather_forecasts/{location_name}/{date_part(now)}/{timestamp_name(now)}.json"
    target_path = data_root / object_name
    target_path.parent.mkdir(parents=True, exist_ok=True)
    target_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return target_path, object_name


def resolve_data_root(path):
    if path.is_absolute():
        return path
    return PROJECT_ROOT / path


def upload_to_gcs(bucket, object_name, path):
    if bucket is None:
        return

    blob = bucket.blob(object_name)
    blob.upload_from_filename(path, content_type="application/json")


def parse_location_spec(value):
    if ":" not in value:
        raise argparse.ArgumentTypeError("expected name:id, such as kitchener_waterloo:on-82")

    location_name, location_id = value.split(":", 1)
    location_name = location_name.strip()
    location_id = location_id.strip()

    if not location_name or not location_id:
        raise argparse.ArgumentTypeError("location name and id are required")

    return location_name, location_id


def configured_locations(args):
    if args.location:
        return args.location

    env_locations = os.getenv("WEATHER_FORECAST_LOCATIONS")
    if env_locations:
        return [parse_location_spec(item.strip()) for item in env_locations.split(",") if item.strip()]

    if args.location_id and args.location_name:
        return [(args.location_name, args.location_id)]

    return list(DEFAULT_LOCATIONS)


def parse_args():
    parser = argparse.ArgumentParser(description="Collect ECCC weather forecast snapshots.")
    parser.add_argument(
        "--location",
        action="append",
        type=parse_location_spec,
        help="Weather location as name:id, such as kitchener_waterloo:on-82. May be repeated.",
    )
    parser.add_argument(
        "--location-id",
        default=os.getenv("WEATHER_FORECAST_LOCATION_ID"),
        help="Deprecated single-location ECCC citypageweather item id.",
    )
    parser.add_argument(
        "--location-name",
        default=os.getenv("WEATHER_FORECAST_LOCATION_NAME"),
        help="Deprecated single-location local/GCS folder name.",
    )
    parser.add_argument(
        "--api-url",
        default=os.getenv("WEATHER_FORECAST_API_URL", DEFAULT_API_URL),
        help="ECCC citypageweather collection URL.",
    )
    parser.add_argument(
        "--data-root",
        type=Path,
        default=Path(os.getenv("DATA_ROOT", DEFAULT_DATA_ROOT)),
        help="Directory where collected data should be stored.",
    )
    return parser.parse_args()


def main():
    load_dotenv(PROJECT_ROOT / ".env")
    args = parse_args()
    data_root = resolve_data_root(args.data_root).resolve()
    gcs_bucket = make_gcs_bucket(os.getenv("GCS_BUCKET"))
    now = utc_now()
    locations = configured_locations(args)

    for location_name, location_id in locations:
        payload = fetch_forecast(location_id, args.api_url)
        hourly_count, last_updated = validate_forecast(payload)
        path, object_name = save_forecast(data_root, location_name, payload, now)

        if gcs_bucket:
            upload_to_gcs(gcs_bucket, object_name, path)
            destination = f"gs://{gcs_bucket.name}/{object_name}"
        else:
            destination = str(path)

        print(
            f"[{timestamp_name(now)}] saved weather_forecast {location_name}: "
            f"{destination} ({hourly_count} hourly forecasts, lastUpdated={last_updated})"
        )


if __name__ == "__main__":
    main()
