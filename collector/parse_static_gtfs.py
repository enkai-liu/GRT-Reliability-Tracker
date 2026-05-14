import argparse
import csv
import io
import os
import zipfile
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv
from google.cloud import storage


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_STATIC_ROOT = PROJECT_ROOT / "data" / "static_gtfs"
DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "data" / "parsed_static_gtfs"
STATIC_FEEDS = (
    "bus_static_gtfs",
    "lrt_static_gtfs",
)
REQUIRED_GTFS_FILES = {
    "routes.txt",
    "stops.txt",
    "trips.txt",
    "stop_times.txt",
}


def normalize_table_name(zip_member_name):
    return Path(zip_member_name).name.removesuffix(".txt")


def find_static_zip_paths(static_root, dates, feeds):
    selected_feeds = feeds or STATIC_FEEDS
    selected_dates = set(dates) if dates else None

    for feed_name in selected_feeds:
        feed_dir = static_root / feed_name
        if not feed_dir.exists():
            continue

        for date_dir in sorted(item for item in feed_dir.iterdir() if item.is_dir()):
            if selected_dates and date_dir.name not in selected_dates:
                continue

            zip_path = date_dir / "GTFS.zip"
            if zip_path.exists():
                yield feed_name, date_dir.name, zip_path


def sync_static_gtfs_from_gcs(bucket_name, static_root, dates, feeds):
    if not bucket_name:
        raise ValueError("GCS bucket is required for --sync-from-gcs")
    if not dates:
        raise ValueError("--date is required with --sync-from-gcs to avoid downloading the whole bucket")

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    selected_feeds = feeds or STATIC_FEEDS
    downloaded = 0

    for feed_name in selected_feeds:
        for snapshot_date in dates:
            object_name = f"static_gtfs/{feed_name}/{snapshot_date}/GTFS.zip"
            blob = bucket.blob(object_name)

            if not blob.exists():
                print(f"Missing gs://{bucket_name}/{object_name}")
                continue

            target_path = static_root / feed_name / snapshot_date / "GTFS.zip"
            target_path.parent.mkdir(parents=True, exist_ok=True)
            blob.download_to_filename(target_path)
            downloaded += 1

    print(f"Downloaded {downloaded} static GTFS ZIPs from gs://{bucket_name}")


def validate_zip(zip_path):
    with zipfile.ZipFile(zip_path) as gtfs_zip:
        names = {Path(name).name for name in gtfs_zip.namelist()}

    missing = sorted(REQUIRED_GTFS_FILES - names)
    if missing:
        raise ValueError(f"{zip_path} is missing required GTFS files: {', '.join(missing)}")


def read_gtfs_txt(gtfs_zip, member_name, feed_name, snapshot_date, source_path):
    with gtfs_zip.open(member_name) as file_obj:
        text = io.TextIOWrapper(file_obj, encoding="utf-8-sig", newline="")
        reader = csv.DictReader(text)

        if reader.fieldnames is None:
            return []

        rows = []
        for row in reader:
            rows.append(
                {
                    "feed_name": feed_name,
                    "snapshot_date": snapshot_date,
                    "source_zip": str(source_path),
                    **{key: (value if value != "" else None) for key, value in row.items()},
                }
            )

    return rows


def write_rows(output_root, table_name, feed_name, snapshot_date, rows, overwrite):
    table_dir = output_root / table_name / f"p_feed={feed_name}" / f"p_snapshot_date={snapshot_date}"
    table_dir.mkdir(parents=True, exist_ok=True)

    output_path = table_dir / "part-000.parquet"
    if output_path.exists() and not overwrite:
        raise FileExistsError(f"{output_path} already exists. Pass --overwrite to replace it.")

    if rows:
        schema = pa.schema([(name, pa.string()) for name in rows[0].keys()])
    else:
        schema = pa.schema(
            [
                ("feed_name", pa.string()),
                ("snapshot_date", pa.string()),
                ("source_zip", pa.string()),
            ]
        )

    table = pa.Table.from_pylist(rows, schema=schema)
    pq.write_table(table, output_path, compression="snappy")
    return output_path


def parse_zip(feed_name, snapshot_date, zip_path, output_root, tables, overwrite):
    validate_zip(zip_path)
    output_paths = []

    with zipfile.ZipFile(zip_path) as gtfs_zip:
        members = sorted(
            name
            for name in gtfs_zip.namelist()
            if Path(name).name.endswith(".txt") and not name.endswith("/")
        )

        for member_name in members:
            table_name = normalize_table_name(member_name)
            if tables and table_name not in tables:
                continue

            rows = read_gtfs_txt(gtfs_zip, member_name, feed_name, snapshot_date, zip_path)
            output_path = write_rows(output_root, table_name, feed_name, snapshot_date, rows, overwrite)
            output_paths.append(output_path)
            print(f"Wrote {len(rows)} rows to {output_path}")

    return output_paths


def upload_parquet_to_gcs(bucket_name, output_root, output_paths):
    if not bucket_name:
        raise ValueError("GCS bucket is required for --upload-to-gcs")

    client = storage.Client()
    bucket = client.bucket(bucket_name)

    for path in output_paths:
        object_name = str(path.relative_to(output_root))
        blob = bucket.blob(f"parsed_static_gtfs/{object_name}")
        blob.upload_from_filename(path, content_type="application/vnd.apache.parquet")
        print(f"Uploaded {path} to gs://{bucket_name}/parsed_static_gtfs/{object_name}")


def parse_args():
    parser = argparse.ArgumentParser(description="Parse static GTFS ZIP files into Parquet tables.")
    parser.add_argument("--static-root", type=Path, default=DEFAULT_STATIC_ROOT, help="Static GTFS ZIP root.")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT, help="Parsed Parquet output root.")
    parser.add_argument("--gcs-bucket", default=None, help="GCS bucket for optional sync/upload operations.")
    parser.add_argument(
        "--sync-from-gcs",
        action="store_true",
        help="Download matching static GTFS ZIPs from GCS before parsing. Requires --date.",
    )
    parser.add_argument("--upload-to-gcs", action="store_true", help="Upload written Parquet files to GCS.")
    parser.add_argument("--date", action="append", help="Date to parse in YYYY-MM-DD form. May be repeated.")
    parser.add_argument(
        "--feed",
        action="append",
        choices=STATIC_FEEDS,
        help="Static feed to parse. May be repeated. Defaults to bus and LRT.",
    )
    parser.add_argument(
        "--table",
        action="append",
        help="GTFS table to parse without .txt, such as routes or stop_times. May be repeated.",
    )
    parser.add_argument("--overwrite", action="store_true", help="Replace existing Parquet outputs.")
    return parser.parse_args()


def main():
    load_dotenv(PROJECT_ROOT / ".env")
    args = parse_args()
    static_root = args.static_root.resolve()
    output_root = args.output_root.resolve()
    gcs_bucket = args.gcs_bucket or os.getenv("GCS_BUCKET")
    output_paths = []
    parsed_files = 0

    if args.sync_from_gcs:
        sync_static_gtfs_from_gcs(gcs_bucket, static_root, args.date, args.feed)

    for feed_name, snapshot_date, zip_path in find_static_zip_paths(static_root, args.date, args.feed):
        print(f"Parsing {zip_path}")
        output_paths.extend(parse_zip(feed_name, snapshot_date, zip_path, output_root, args.table, args.overwrite))
        parsed_files += 1

    if parsed_files == 0:
        print(f"No static GTFS ZIPs found under {static_root}")
        raise SystemExit(1)

    print(f"Parsed {parsed_files} static GTFS ZIPs from {static_root}")

    if args.upload_to_gcs:
        upload_parquet_to_gcs(gcs_bucket, output_root, output_paths)


if __name__ == "__main__":
    main()
