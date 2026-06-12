"""Export a compact one-day timetable for the in-browser trip planner.

Reads the latest static GTFS snapshot (bus + LRT), resolves which services run
on the requested date, and writes a RAPTOR-friendly timetable JSON:

  - stops: parallel arrays (id, name, lat, lon) for every stop served that day
  - patterns: unique (route, direction, stop sequence) with all trips' stop
    times as seconds since midnight (25:10:00-style times allowed, > 86400)
  - footpaths: walking transfers between nearby stops
  - reliability: per route+hour delay quantiles and transfer make-rates, so
    the client can rank itineraries by predicted (not just scheduled) arrival

The output is dashboard/data/timetable.json. Regenerate daily alongside the
dashboard export.
"""

import argparse
import json
import math
from collections import defaultdict
from datetime import date, datetime, timezone
from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_STATIC_ROOT = PROJECT_ROOT / "data" / "parsed_static_gtfs"
DEFAULT_RELIABILITY_ROOT = PROJECT_ROOT / "data" / "analysis" / "reliability"
DEFAULT_TRANSFERS_ROOT = PROJECT_ROOT / "data" / "analysis" / "transfers"
DEFAULT_OUTPUT = PROJECT_ROOT / "dashboard" / "data" / "timetable.json"

FEEDS = {"bus_static_gtfs": "bus", "lrt_static_gtfs": "lrt"}

WALK_SPEED_M_S = 1.25
FOOTPATH_MAX_M = 250.0
FOOTPATH_PENALTY_S = 30
MIN_TRANSFER_ATTEMPTS = 25


def iso_date(value):
    return date.fromisoformat(value).isoformat()


def parse_gtfs_seconds(value):
    h, m, s = value.split(":")
    return int(h) * 3600 + int(m) * 60 + int(s)


def latest_snapshot(con, static_root, feed, table):
    row = con.sql(f"""
        SELECT max(p_snapshot_date)
        FROM read_parquet('{static_root}/{table}/p_feed={feed}/p_snapshot_date=*/part-000.parquet',
                          hive_partitioning=true)
    """).fetchone()
    return row[0]


def feed_has_table(static_root, feed, table):
    return any((static_root / table).glob(f"p_feed={feed}/p_snapshot_date=*/part-000.parquet"))


def active_service_ids(con, static_root, feed, snapshot, service_date):
    """Resolve calendar + calendar_dates for one feed and date."""
    services = set()
    weekday_col = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"][
        date.fromisoformat(service_date).weekday()
    ]
    compact = service_date.replace("-", "")

    if feed_has_table(static_root, feed, "calendar"):
        rows = con.sql(f"""
            SELECT service_id
            FROM read_parquet('{static_root}/calendar/p_feed={feed}/p_snapshot_date=*/part-000.parquet',
                              hive_partitioning=true)
            WHERE p_snapshot_date = DATE '{snapshot}'
              AND {weekday_col} = '1'
              AND start_date <= '{compact}'
              AND end_date >= '{compact}'
        """).fetchall()
        services.update(r[0] for r in rows)

    if feed_has_table(static_root, feed, "calendar_dates"):
        rows = con.sql(f"""
            SELECT service_id, exception_type
            FROM read_parquet('{static_root}/calendar_dates/p_feed={feed}/p_snapshot_date=*/part-000.parquet',
                              hive_partitioning=true)
            WHERE p_snapshot_date = DATE '{snapshot}'
              AND date = '{compact}'
        """).fetchall()
        for service_id, exception_type in rows:
            if str(exception_type) == "1":
                services.add(service_id)
            elif str(exception_type) == "2":
                services.discard(service_id)

    return services


def load_feed(con, static_root, feed, mode, service_date):
    snapshot = latest_snapshot(con, static_root, feed, "trips")
    if snapshot is None:
        return [], {}

    services = active_service_ids(con, static_root, feed, snapshot, service_date)
    if not services:
        return [], {}

    service_list = ", ".join(f"'{s}'" for s in services)
    trips = con.sql(f"""
        SELECT trip_id, route_id, COALESCE(trip_headsign, '') AS headsign,
               COALESCE(direction_id, '0') AS direction_id
        FROM read_parquet('{static_root}/trips/p_feed={feed}/p_snapshot_date=*/part-000.parquet',
                          hive_partitioning=true)
        WHERE p_snapshot_date = DATE '{snapshot}'
          AND service_id IN ({service_list})
    """).fetchall()
    trip_meta = {t[0]: (t[1], t[2], t[3]) for t in trips}
    if not trip_meta:
        return [], {}

    stop_times = con.sql(f"""
        SELECT st.trip_id, CAST(st.stop_sequence AS INTEGER) AS seq,
               st.stop_id, st.departure_time, st.arrival_time
        FROM read_parquet('{static_root}/stop_times/p_feed={feed}/p_snapshot_date=*/part-000.parquet',
                          hive_partitioning=true) st
        SEMI JOIN (
            SELECT trip_id
            FROM read_parquet('{static_root}/trips/p_feed={feed}/p_snapshot_date=*/part-000.parquet',
                              hive_partitioning=true)
            WHERE p_snapshot_date = DATE '{snapshot}'
              AND service_id IN ({service_list})
        ) t USING (trip_id)
        WHERE st.p_snapshot_date = DATE '{snapshot}'
        ORDER BY st.trip_id, seq
    """).fetchall()

    by_trip = defaultdict(list)
    for trip_id, _seq, stop_id, dep, arr in stop_times:
        seconds = parse_gtfs_seconds(dep or arr)
        by_trip[trip_id].append((stop_id, seconds))

    stops = con.sql(f"""
        SELECT stop_id, any_value(stop_name), any_value(CAST(stop_lat AS DOUBLE)),
               any_value(CAST(stop_lon AS DOUBLE))
        FROM read_parquet('{static_root}/stops/p_feed={feed}/p_snapshot_date=*/part-000.parquet',
                          hive_partitioning=true)
        WHERE p_snapshot_date = DATE '{snapshot}'
        GROUP BY stop_id
    """).fetchall()
    stop_meta = {s[0]: (s[1], s[2], s[3]) for s in stops}

    trips_out = []
    for trip_id, events in by_trip.items():
        if len(events) < 2:
            continue
        route_id, headsign, direction_id = trip_meta[trip_id]
        trips_out.append({
            "trip_id": trip_id,
            "routeKey": f"{mode}:{route_id}",
            "headsign": headsign,
            "direction": direction_id,
            "events": events,
        })
    return trips_out, stop_meta


def haversine_m(lat1, lon1, lat2, lon2):
    r = 6371000.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = p2 - p1
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def build_footpaths(stop_ids, coords):
    """All walking links between stops within FOOTPATH_MAX_M, via a degree grid."""
    cell = 0.004  # ~400 m
    grid = defaultdict(list)
    for idx, stop_id in enumerate(stop_ids):
        lat, lon = coords[idx]
        grid[(int(lat / cell), int(lon / cell))].append(idx)

    links = []
    for idx, stop_id in enumerate(stop_ids):
        lat, lon = coords[idx]
        gx, gy = int(lat / cell), int(lon / cell)
        for dx in (-1, 0, 1):
            for dy in (-1, 0, 1):
                for jdx in grid.get((gx + dx, gy + dy), ()):
                    if jdx <= idx:
                        continue
                    lat2, lon2 = coords[jdx]
                    dist = haversine_m(lat, lon, lat2, lon2)
                    if dist <= FOOTPATH_MAX_M:
                        seconds = int(dist / WALK_SPEED_M_S) + FOOTPATH_PENALTY_S
                        links.append([idx, jdx, seconds])
    return links


def load_reliability(con, reliability_root):
    """Per route+hour delay quantiles: routeKey -> hour -> [median, p90]."""
    path = reliability_root / "route_by_hour.parquet"
    if not path.exists():
        return {}
    rows = con.sql(f"""
        SELECT transit_mode, route_id, hour_of_day,
               median_delay_seconds, p90_delay_seconds, observations
        FROM read_parquet('{path}')
        WHERE observations >= 30
    """).fetchall()
    out = defaultdict(dict)
    for mode, route_id, hour, median, p90, _obs in rows:
        out[f"{mode}:{route_id}"][str(int(hour))] = [round(median), round(p90)]
    return dict(out)


def load_transfer_rates(con, transfers_root):
    """Observed make-rates: 'fromRoute|toRoute|fromStop' -> success rate."""
    path = transfers_root / "transfer_stop_summary.parquet"
    if not path.exists():
        return {}
    rows = con.sql(f"""
        SELECT from_mode, from_route_id, to_mode, to_route_id, from_stop_id,
               success_rate, attempts
        FROM read_parquet('{path}')
        WHERE attempts >= {MIN_TRANSFER_ATTEMPTS}
    """).fetchall()
    out = {}
    for from_mode, from_route, to_mode, to_route, from_stop, rate, _attempts in rows:
        key = f"{from_mode}:{from_route}|{to_mode}:{to_route}|{from_stop}"
        out[key] = round(rate, 3)
    return out


def build_payload(static_root, reliability_root, transfers_root, service_date):
    con = duckdb.connect()
    con.sql("SET timezone = 'UTC'")

    all_trips = []
    stop_meta = {}
    for feed, mode in FEEDS.items():
        trips, stops = load_feed(con, static_root, feed, mode, service_date)
        all_trips.extend(trips)
        stop_meta.update(stops)

    if not all_trips:
        raise SystemExit(f"No trips found for {service_date}")

    # index the stops actually served
    used = set()
    for trip in all_trips:
        for stop_id, _ in trip["events"]:
            used.add(stop_id)
    stop_ids = sorted(s for s in used if s in stop_meta and stop_meta[s][1] is not None)
    stop_index = {stop_id: idx for idx, stop_id in enumerate(stop_ids)}
    coords = [(stop_meta[s][1], stop_meta[s][2]) for s in stop_ids]

    # group trips into patterns (route + direction + exact stop sequence)
    patterns = {}
    for trip in all_trips:
        events = [(stop_index[s], t) for s, t in trip["events"] if s in stop_index]
        if len(events) < 2:
            continue
        seq = tuple(idx for idx, _ in events)
        key = (trip["routeKey"], trip["direction"], seq)
        entry = patterns.setdefault(key, {
            "routeKey": trip["routeKey"],
            "direction": trip["direction"],
            "headsign": trip["headsign"],
            "stops": list(seq),
            "trips": [],
        })
        entry["trips"].append({
            "id": trip["trip_id"],
            "times": [t for _, t in events],
        })

    pattern_list = []
    for entry in patterns.values():
        entry["trips"].sort(key=lambda t: t["times"][0])
        pattern_list.append(entry)
    pattern_list.sort(key=lambda p: (p["routeKey"], p["direction"]))

    return {
        "generatedAtUtc": datetime.now(timezone.utc).isoformat(),
        "serviceDate": service_date,
        "walkSpeedMS": WALK_SPEED_M_S,
        "stops": {
            "ids": stop_ids,
            "names": [stop_meta[s][0] for s in stop_ids],
            "lats": [round(stop_meta[s][1], 6) for s in stop_ids],
            "lons": [round(stop_meta[s][2], 6) for s in stop_ids],
        },
        "patterns": pattern_list,
        "footpaths": build_footpaths(stop_ids, coords),
        "reliability": {
            "routeHour": load_reliability(con, reliability_root),
            "transferRates": load_transfer_rates(con, transfers_root),
        },
    }


def parse_args():
    parser = argparse.ArgumentParser(description="Export the trip planner timetable JSON.")
    parser.add_argument("--static-root", type=Path, default=DEFAULT_STATIC_ROOT)
    parser.add_argument("--reliability-root", type=Path, default=DEFAULT_RELIABILITY_ROOT)
    parser.add_argument("--transfers-root", type=Path, default=DEFAULT_TRANSFERS_ROOT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--date", type=iso_date, default=date.today().isoformat(),
                        help="Service date to export (YYYY-MM-DD). Defaults to today.")
    return parser.parse_args()


def main():
    args = parse_args()
    payload = build_payload(
        args.static_root.resolve(), args.reliability_root.resolve(),
        args.transfers_root.resolve(), args.date,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")
    trips = sum(len(p["trips"]) for p in payload["patterns"])
    print(
        f"Wrote {args.output} — {len(payload['stops']['ids'])} stops, "
        f"{len(payload['patterns'])} patterns, {trips} trips, "
        f"{len(payload['footpaths'])} footpaths for {args.date}"
    )


if __name__ == "__main__":
    main()
