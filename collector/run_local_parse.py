import argparse
import subprocess
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def parse_date(value):
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise argparse.ArgumentTypeError("expected YYYY-MM-DD") from exc


def yesterday_utc():
    return (datetime.now(timezone.utc).date() - timedelta(days=1))


def date_range(start_date, end_date):
    current = start_date
    while current <= end_date:
        yield current
        current += timedelta(days=1)


def run_command(command):
    print("Running:", " ".join(str(part) for part in command), flush=True)
    subprocess.run(command, cwd=PROJECT_ROOT, check=True)


def parse_day(day, skip_static):
    day_text = day.isoformat()
    python = sys.executable

    run_command(
        [
            python,
            "collector/parse_snapshots.py",
            "--date",
            day_text,
            "--sync-from-gcs",
            "--upload-to-gcs",
            "--overwrite",
        ]
    )

    if not skip_static:
        run_command(
            [
                python,
                "collector/parse_static_gtfs.py",
                "--date",
                day_text,
                "--sync-from-gcs",
                "--upload-to-gcs",
                "--overwrite",
            ]
        )


def parse_args():
    parser = argparse.ArgumentParser(description="Parse GRT raw snapshots locally and upload Parquet to GCS.")
    parser.add_argument(
        "--date",
        action="append",
        type=parse_date,
        help="Date to parse in YYYY-MM-DD form. May be repeated. Defaults to yesterday UTC.",
    )
    parser.add_argument("--start-date", type=parse_date, help="First date to parse in YYYY-MM-DD form.")
    parser.add_argument("--end-date", type=parse_date, help="Last date to parse in YYYY-MM-DD form.")
    parser.add_argument(
        "--skip-static",
        action="store_true",
        help="Only parse GTFS-Realtime snapshots; skip static GTFS ZIP parsing.",
    )
    return parser.parse_args()


def selected_dates(args):
    if args.date and (args.start_date or args.end_date):
        raise SystemExit("Use either --date or --start-date/--end-date, not both.")

    if args.date:
        return sorted(set(args.date))

    if args.start_date or args.end_date:
        if not args.start_date or not args.end_date:
            raise SystemExit("--start-date and --end-date must be used together.")
        if args.end_date < args.start_date:
            raise SystemExit("--end-date must be on or after --start-date.")
        return list(date_range(args.start_date, args.end_date))

    return [yesterday_utc()]


def main():
    load_dotenv(PROJECT_ROOT / ".env")
    args = parse_args()
    days = selected_dates(args)

    for day in days:
        print(f"Parsing {day.isoformat()}", flush=True)
        parse_day(day, args.skip_static)


if __name__ == "__main__":
    main()
