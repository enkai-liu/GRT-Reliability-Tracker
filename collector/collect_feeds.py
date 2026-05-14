import argparse
import os
import ssl
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv
from google.cloud import storage
from google.protobuf.message import DecodeError
from google.transit import gtfs_realtime_pb2
from requests.adapters import HTTPAdapter
from urllib3.poolmanager import PoolManager


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DATA_ROOT = PROJECT_ROOT / "data"
DEFAULT_POLL_SECONDS = 30
REQUEST_TIMEOUT_SECONDS = 20
CONTENT_TYPES = {
    ".pb": "application/x-protobuf",
    ".zip": "application/zip",
}

DEFAULT_FEEDS = {
    "bus_trip_updates": {
        "env": "BUS_TRIP_UPDATES_URL",
        "url": "https://webapps.regionofwaterloo.ca/api/grt-routes/api/tripupdates/1",
        "extension": ".pb",
        "kind": "realtime",
    },
    "bus_vehicle_positions": {
        "env": "BUS_VEHICLE_POSITIONS_URL",
        "url": "https://webapps.regionofwaterloo.ca/api/grt-routes/api/vehiclepositions/1",
        "extension": ".pb",
        "kind": "realtime",
    },
    "lrt_trip_updates": {
        "env": "LRT_TRIP_UPDATES_URL",
        "url": "https://webapps.regionofwaterloo.ca/api/grt-routes/api/tripupdates/2",
        "extension": ".pb",
        "kind": "realtime",
    },
    "lrt_vehicle_positions": {
        "env": "LRT_VEHICLE_POSITIONS_URL",
        "url": "https://webapps.regionofwaterloo.ca/api/grt-routes/api/vehiclepositions/2",
        "extension": ".pb",
        "kind": "realtime",
    },
    "grt_service_alerts": {
        "env": "GRT_SERVICE_ALERTS_URL",
        "url": "https://webapps.regionofwaterloo.ca/api/grt-routes/api/alerts",
        "extension": ".pb",
        "kind": "realtime",
    },
    "bus_static_gtfs": {
        "env": "BUS_STATIC_GTFS_URL",
        "url": "https://webapps.regionofwaterloo.ca/api/grt-routes/api/staticfeeds/1",
        "extension": ".zip",
        "kind": "static",
    },
    "lrt_static_gtfs": {
        "env": "LRT_STATIC_GTFS_URL",
        "url": "https://webapps.regionofwaterloo.ca/api/grt-routes/api/staticfeeds/2",
        "extension": ".zip",
        "kind": "static",
    },
}


class LegacyTlsAdapter(HTTPAdapter):
    """Allow the official GRT feed server's older TLS parameters."""

    def init_poolmanager(self, connections, maxsize, block=False, **pool_kwargs):
        context = ssl.create_default_context()
        context.set_ciphers("DEFAULT@SECLEVEL=1")
        self.poolmanager = PoolManager(
            num_pools=connections,
            maxsize=maxsize,
            block=block,
            ssl_context=context,
            **pool_kwargs,
        )


def make_session():
    session = requests.Session()
    session.mount("https://webapps.regionofwaterloo.ca", LegacyTlsAdapter())
    return session


def make_gcs_bucket(bucket_name):
    if not bucket_name:
        return None

    client = storage.Client()
    return client.bucket(bucket_name)


def utc_now():
    return datetime.now(timezone.utc)


def timestamp_name(now, extension):
    return now.strftime("%Y-%m-%dT%H-%M-%SZ") + extension


def date_part(now):
    return now.strftime("%Y-%m-%d")


def configured_feeds():
    feeds = {}

    for name, config in DEFAULT_FEEDS.items():
        feeds[name] = {
            **config,
            "url": os.getenv(config["env"], config["url"]),
        }

    return feeds


def fetch_bytes(session, url):
    response = session.get(url, timeout=REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()
    return response.content


def is_valid_realtime(content):
    feed = gtfs_realtime_pb2.FeedMessage()
    try:
        feed.ParseFromString(content)
    except DecodeError:
        return False, "protobuf decode failed"

    return True, f"{len(feed.entity)} entities"


def is_valid_zip(path):
    return zipfile.is_zipfile(path)


def save_feed(data_root, name, content, now, extension):
    object_name = f"raw/{name}/{date_part(now)}/{timestamp_name(now, extension)}"
    target_dir = data_root / Path(object_name).parent
    target_dir.mkdir(parents=True, exist_ok=True)

    target_path = data_root / object_name
    target_path.write_bytes(content)
    return target_path, object_name


def save_static_gtfs(data_root, name, content, now):
    object_name = f"static_gtfs/{name}/{date_part(now)}/GTFS.zip"
    target_dir = data_root / Path(object_name).parent
    target_dir.mkdir(parents=True, exist_ok=True)

    target_path = data_root / object_name
    target_path.write_bytes(content)
    return target_path, object_name


def static_already_saved_today(data_root, name, now):
    return (data_root / "static_gtfs" / name / date_part(now) / "GTFS.zip").exists()


def upload_to_gcs(bucket, object_name, path, extension):
    if bucket is None:
        return

    blob = bucket.blob(object_name)
    blob.upload_from_filename(path, content_type=CONTENT_TYPES.get(extension))


def collect_realtime(session, gcs_bucket, data_root, feeds):
    now = utc_now()

    for name, config in feeds.items():
        if config["kind"] != "realtime":
            continue

        try:
            content = fetch_bytes(session, config["url"])
            valid, details = is_valid_realtime(content)

            if not valid:
                print(f"[{timestamp_name(now, '')}] skipped {name}: {details}")
                continue

            path, object_name = save_feed(data_root, name, content, now, config["extension"])

            if gcs_bucket:
                try:
                    upload_to_gcs(gcs_bucket, object_name, path, config["extension"])
                    destination = f"gs://{gcs_bucket.name}/{object_name}"
                    print(f"[{timestamp_name(now, '')}] saved {name}: {destination} ({details})")
                except Exception as exc:
                    print(
                        f"[{timestamp_name(now, '')}] saved {name} locally: {path} "
                        f"({details}); GCS upload failed: {exc}"
                    )
            else:
                print(f"[{timestamp_name(now, '')}] saved {name}: {path} ({details})")
        except requests.RequestException as exc:
            print(f"[{timestamp_name(now, '')}] failed {name}: {exc}")


def collect_static_once_per_day(session, gcs_bucket, data_root, feeds):
    now = utc_now()

    for name, config in feeds.items():
        if config["kind"] != "static":
            continue

        if static_already_saved_today(data_root, name, now):
            continue

        try:
            content = fetch_bytes(session, config["url"])
            path, object_name = save_static_gtfs(data_root, name, content, now)

            if not is_valid_zip(path):
                path.unlink(missing_ok=True)
                print(f"[{timestamp_name(now, '')}] skipped {name}: invalid zip")
                continue

            if gcs_bucket:
                try:
                    upload_to_gcs(gcs_bucket, object_name, path, config["extension"])
                    destination = f"gs://{gcs_bucket.name}/{object_name}"
                    print(f"[{timestamp_name(now, '')}] saved {name}: {destination}")
                except Exception as exc:
                    print(
                        f"[{timestamp_name(now, '')}] saved {name} locally: {path}; "
                        f"GCS upload failed: {exc}"
                    )
            else:
                print(f"[{timestamp_name(now, '')}] saved {name}: {path}")
        except requests.RequestException as exc:
            print(f"[{timestamp_name(now, '')}] failed {name}: {exc}")


def positive_int(value):
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be greater than zero")
    return parsed


def parse_args():
    parser = argparse.ArgumentParser(description="Collect GRT GTFS-Realtime and static GTFS snapshots.")
    parser.add_argument(
        "--once",
        action="store_true",
        help="Collect one realtime batch and today's static GTFS files, then exit.",
    )
    parser.add_argument(
        "--poll-seconds",
        type=positive_int,
        default=int(os.getenv("POLL_SECONDS", DEFAULT_POLL_SECONDS)),
        help="Seconds between realtime polling cycles.",
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
    feeds = configured_feeds()
    data_root = args.data_root.resolve()
    session = make_session()
    gcs_bucket = make_gcs_bucket(os.getenv("GCS_BUCKET"))

    print(f"Writing data to: {data_root}")
    print(f"Realtime poll interval: {args.poll_seconds} seconds")
    if gcs_bucket:
        print(f"Uploading snapshots to: gs://{gcs_bucket.name}")

    while True:
        collect_static_once_per_day(session, gcs_bucket, data_root, feeds)
        collect_realtime(session, gcs_bucket, data_root, feeds)

        if args.once:
            break

        time.sleep(args.poll_seconds)


if __name__ == "__main__":
    main()
