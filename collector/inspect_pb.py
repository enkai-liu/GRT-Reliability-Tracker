import argparse
import zipfile
from datetime import datetime, timezone
from pathlib import Path

from google.transit import gtfs_realtime_pb2


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DATA_ROOT = PROJECT_ROOT / "data"
REQUIRED_GTFS_FILES = {
    "routes.txt",
    "stops.txt",
    "trips.txt",
    "stop_times.txt",
}


def format_timestamp(timestamp):
    if not timestamp:
        return "N/A"

    return datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat()


def inspect_feed(path, entity_limit, stop_update_limit):
    path = Path(path)

    if not path.exists():
        print(f"File not found: {path}")
        return False

    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(path.read_bytes())

    print("\n" + "=" * 80)
    print(f"File: {path}")
    print(f"Feed timestamp: {feed.header.timestamp}")
    print(f"Feed time UTC: {format_timestamp(feed.header.timestamp)}")
    print(f"Number of entities: {len(feed.entity)}")

    for entity in feed.entity[:entity_limit]:
        print("\n--- Entity ---")
        print("Entity ID:", entity.id)

        if entity.HasField("vehicle"):
            vehicle = entity.vehicle
            print("Type: Vehicle Position")
            print("Trip ID:", vehicle.trip.trip_id)
            print("Route ID:", vehicle.trip.route_id)
            print("Vehicle ID:", vehicle.vehicle.id)
            print("Latitude:", vehicle.position.latitude)
            print("Longitude:", vehicle.position.longitude)
            print("Bearing:", vehicle.position.bearing)
            print("Speed:", vehicle.position.speed)
            print("Timestamp:", format_timestamp(vehicle.timestamp))

        elif entity.HasField("trip_update"):
            trip_update = entity.trip_update
            print("Type: Trip Update")
            print("Trip ID:", trip_update.trip.trip_id)
            print("Route ID:", trip_update.trip.route_id)
            print("Stop updates:", len(trip_update.stop_time_update))

            for stop_update in trip_update.stop_time_update[:stop_update_limit]:
                print("  Stop ID:", stop_update.stop_id)
                print("  Stop sequence:", stop_update.stop_sequence)

                if stop_update.arrival.HasField("delay"):
                    print("  Arrival delay seconds:", stop_update.arrival.delay)

                if stop_update.arrival.HasField("time"):
                    print("  Arrival time UTC:", format_timestamp(stop_update.arrival.time))

                if stop_update.departure.HasField("delay"):
                    print("  Departure delay seconds:", stop_update.departure.delay)

                if stop_update.departure.HasField("time"):
                    print("  Departure time UTC:", format_timestamp(stop_update.departure.time))

        elif entity.HasField("alert"):
            print("Type: Alert")

        else:
            print("Type: Unknown")

    return True


def inspect_static_gtfs(path):
    path = Path(path)

    if not path.exists():
        print(f"\nStatic GTFS not found: {path}")
        return False

    if path.is_dir():
        names = {item.name for item in path.iterdir() if item.is_file()}
    elif zipfile.is_zipfile(path):
        with zipfile.ZipFile(path) as gtfs_zip:
            names = {Path(name).name for name in gtfs_zip.namelist()}
    else:
        print(f"\nStatic GTFS path is not a directory or zip file: {path}")
        return False

    missing = sorted(REQUIRED_GTFS_FILES - names)

    print("\n" + "=" * 80)
    print(f"Static GTFS: {path}")

    if missing:
        print("Missing required files:", ", ".join(missing))
        return False

    print("Required files found:", ", ".join(sorted(REQUIRED_GTFS_FILES)))
    return True


def parse_args():
    parser = argparse.ArgumentParser(description="Inspect GTFS-Realtime protobuf or static GTFS files.")
    parser.add_argument("paths", nargs="+", type=Path, help="One or more .pb, .zip, or GTFS directory paths.")
    parser.add_argument("--entity-limit", type=int, default=5, help="Realtime entities to preview per feed.")
    parser.add_argument("--stop-update-limit", type=int, default=3, help="Stop updates to preview per trip update.")
    return parser.parse_args()


def main():
    args = parse_args()
    results = []

    for path in args.paths:
        if path.suffix == ".pb":
            results.append(inspect_feed(path, args.entity_limit, args.stop_update_limit))
        else:
            results.append(inspect_static_gtfs(path))

    raise SystemExit(0 if all(results) else 1)


if __name__ == "__main__":
    main()
