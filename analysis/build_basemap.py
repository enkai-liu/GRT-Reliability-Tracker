#!/usr/bin/env python3
"""Build a compact vector basemap for the Reliability Atlas map.

Fetches Waterloo Region geography from OpenStreetMap (via the Overpass API),
simplifies it, and writes ``dashboard/data/basemap.json`` — a self-contained
layer the dashboard draws *under* the colored transit routes, using the same
projection so everything aligns by construction.

What it pulls (kept deliberately sparse so the colored routes stay the hero):
  - water     : lake / reservoir / wide-river polygons (natural=water)
  - rivers    : river + canal centerlines (waterway=river|canal)
  - roads     : major roads, split into "highway" (motorway/trunk) and
                "arterial" (primary/secondary)
  - boundary  : the Region of Waterloo outline ("region") and its lower-tier
                municipalities ("municipal")

Coordinates are emitted as ``[lat, lon]`` pairs (matching ``routeShapes`` in
dashboard-data.json) rounded to 5 decimals (~1 m), after Douglas-Peucker
simplification, to keep the shipped file small.

Usage:
    collector/.venv/bin/python analysis/build_basemap.py
    collector/.venv/bin/python analysis/build_basemap.py --bbox 43.33,-80.72,43.60,-80.27

The bbox defaults to the route-network bounds in dashboard-data.json, expanded
by a small margin so the basemap bleeds slightly past the network edges.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
DASHBOARD_DATA = REPO / "dashboard" / "data"
OUT_PATH = DASHBOARD_DATA / "basemap.json"
CACHE_DIR = REPO / "data" / "basemap_cache"  # raw Overpass responses (gitignored)

# Overpass mirrors, tried in order on failure.
OVERPASS_ENDPOINTS = [
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
    "https://maps.mail.ru/osm/tools/overpass/api/interpreter",
]
USER_AGENT = "grt-reliability-tracker/1.0 (basemap build; +https://github.com/enkai-liu/GRT-Reliability-Tracker)"

# Simplification tolerance in degrees (~1.5 deg-second ≈ 11 m N-S).
SIMPLIFY_TOL = 0.00015
BOUNDARY_TOL = 0.0006  # faint reference lines — simplify harder
COORD_DECIMALS = 5
# Drop water polygons smaller than this in either span (~250 m) so the file
# carries lakes/reservoirs/wide rivers, not thousands of stormwater ponds.
MIN_WATER_SPAN = 0.0025


# --------------------------------------------------------------------------- #
# Geometry helpers
# --------------------------------------------------------------------------- #
def _perp_dist_sq(p, a, b):
    """Squared perpendicular distance from point p to segment a-b (planar)."""
    (py, px), (ay, ax), (by, bx) = p, a, b
    dx, dy = bx - ax, by - ay
    if dx == 0 and dy == 0:
        return (px - ax) ** 2 + (py - ay) ** 2
    t = ((px - ax) * dx + (py - ay) * dy) / (dx * dx + dy * dy)
    t = max(0.0, min(1.0, t))
    cx, cy = ax + t * dx, ay + t * dy
    return (px - cx) ** 2 + (py - cy) ** 2


def simplify(points, tol=SIMPLIFY_TOL):
    """Iterative Douglas-Peucker (avoids recursion limits on long ways)."""
    if len(points) < 3:
        return points
    tol_sq = tol * tol
    keep = [False] * len(points)
    keep[0] = keep[-1] = True
    stack = [(0, len(points) - 1)]
    while stack:
        lo, hi = stack.pop()
        if hi <= lo + 1:
            continue
        max_d, idx = -1.0, -1
        a, b = points[lo], points[hi]
        for i in range(lo + 1, hi):
            d = _perp_dist_sq(points[i], a, b)
            if d > max_d:
                max_d, idx = d, i
        if max_d > tol_sq:
            keep[idx] = True
            stack.append((lo, idx))
            stack.append((idx, hi))
    return [p for p, k in zip(points, keep) if k]


def round_line(points):
    return [[round(lat, COORD_DECIMALS), round(lon, COORD_DECIMALS)] for lat, lon in points]


def stitch_rings(ways, tol=1e-6):
    """Assemble OSM multipolygon member ways into rings.

    A single outer ring is frequently split across several member ways that
    share end nodes. Appending each way on its own leaves open chains, which
    the dashboard fills by joining the loose ends with a straight chord — a
    long member gap then slashes a wedge across the map. This greedily joins
    ways by matching endpoints (reversing where needed) until each ring closes
    or no further match is found. ``tol`` is in degrees; only genuinely shared
    nodes (distance ~0) match, so distinct features never merge.
    """
    pool = [list(w) for w in ways if len(w) >= 2]
    used = [False] * len(pool)

    def near(a, b):
        return abs(a[0] - b[0]) <= tol and abs(a[1] - b[1]) <= tol

    rings = []
    for i in range(len(pool)):
        if used[i]:
            continue
        ring = list(pool[i])
        used[i] = True
        extended = True
        while extended and not near(ring[0], ring[-1]):
            extended = False
            for j in range(len(pool)):
                if used[j]:
                    continue
                w = pool[j]
                if near(ring[-1], w[0]):
                    ring.extend(w[1:])
                elif near(ring[-1], w[-1]):
                    ring.extend(reversed(w[:-1]))
                elif near(ring[0], w[-1]):
                    ring[:0] = w[:-1]
                elif near(ring[0], w[0]):
                    ring[:0] = list(reversed(w))[:-1]
                else:
                    continue
                used[j] = True
                extended = True
                break
        rings.append(ring)
    return rings


# --------------------------------------------------------------------------- #
# Overpass
# --------------------------------------------------------------------------- #
def overpass(query: str) -> dict:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    key = hashlib.sha1(query.encode()).hexdigest()[:16]
    cache = CACHE_DIR / f"{key}.json"
    if cache.exists():
        print(f"  (cached {cache.name})")
        return json.loads(cache.read_text())

    data = urllib.parse.urlencode({"data": query}).encode()
    last_err = None
    for endpoint in OVERPASS_ENDPOINTS:
        for attempt in range(3):
            try:
                req = urllib.request.Request(
                    endpoint, data=data, headers={"User-Agent": USER_AGENT}
                )
                with urllib.request.urlopen(req, timeout=180) as resp:
                    result = json.loads(resp.read().decode())
                cache.write_text(json.dumps(result))
                return result
            except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError) as exc:
                last_err = exc
                wait = 5 * (attempt + 1)
                print(f"  ! {endpoint} failed ({exc}); retry in {wait}s")
                time.sleep(wait)
        print(f"  → switching mirror")
    raise SystemExit(f"All Overpass mirrors failed: {last_err}")


def way_geom(el):
    """[lat, lon] points from an Overpass element with `out geom`."""
    return [(g["lat"], g["lon"]) for g in el.get("geometry", [])]


def big_enough(points, min_span=MIN_WATER_SPAN):
    """True if the polygon spans at least `min_span` deg in lat or lon."""
    lats = [p[0] for p in points]
    lons = [p[1] for p in points]
    return (max(lats) - min(lats)) >= min_span or (max(lons) - min(lons)) >= min_span


# --------------------------------------------------------------------------- #
# Layer builders
# --------------------------------------------------------------------------- #
def fetch_water(bbox: str):
    q = f"""[out:json][timeout:180];
(
  way["natural"="water"]({bbox});
  way["waterway"="riverbank"]({bbox});
  relation["natural"="water"]({bbox});
);
out geom;"""
    els = overpass(q).get("elements", [])
    polys = []
    for el in els:
        if el["type"] == "way":
            pts = way_geom(el)
            if len(pts) >= 4 and big_enough(pts):
                polys.append(simplify(pts))
        elif el["type"] == "relation":
            # An outer ring is often split across several member ways; stitch
            # them back into closed rings before treating each as a polygon,
            # otherwise the open chains fill as straight chords across the map.
            outers = [
                [(g["lat"], g["lon"]) for g in m["geometry"]]
                for m in el.get("members", [])
                if m.get("role") == "outer" and m.get("geometry")
            ]
            for ring in stitch_rings(outers):
                if len(ring) >= 4 and big_enough(ring):
                    polys.append(simplify(ring))
    return [round_line(p) for p in polys]


def fetch_rivers(bbox: str):
    q = f"""[out:json][timeout:180];
way["waterway"~"^(river|canal)$"]({bbox});
out geom;"""
    els = overpass(q).get("elements", [])
    lines = []
    for el in els:
        pts = way_geom(el)
        if len(pts) >= 2:
            lines.append(round_line(simplify(pts)))
    return lines


def fetch_roads(bbox: str):
    # motorway/trunk = "highway"; primary = "arterial". `secondary` is dropped:
    # across seven municipalities it adds ~4.5k rural ways and buries the routes.
    q = f"""[out:json][timeout:180];
way["highway"~"^(motorway|trunk|primary)$"]({bbox});
out geom;"""
    els = overpass(q).get("elements", [])
    highway, arterial = [], []
    for el in els:
        pts = way_geom(el)
        if len(pts) < 2:
            continue
        line = round_line(simplify(pts))
        cls = el.get("tags", {}).get("highway", "")
        if cls in ("motorway", "trunk"):
            highway.append(line)
        else:
            arterial.append(line)
    return {"highway": highway, "arterial": arterial}


def fetch_boundaries(bbox: str):
    q = f"""[out:json][timeout:180];
relation["boundary"="administrative"]["admin_level"~"^(6|8)$"]({bbox});
out geom;"""
    els = overpass(q).get("elements", [])
    region, municipal = [], []
    for el in els:
        level = el.get("tags", {}).get("admin_level")
        bucket = region if level == "6" else municipal
        for m in el.get("members", []):
            if m.get("type") == "way" and m.get("geometry"):
                pts = [(g["lat"], g["lon"]) for g in m["geometry"]]
                if len(pts) >= 2:
                    bucket.append(round_line(simplify(pts, BOUNDARY_TOL)))
    return {"region": region, "municipal": municipal}


# --------------------------------------------------------------------------- #
def default_bbox() -> str:
    """Route-network bounds from dashboard-data.json, expanded by a margin."""
    data = json.loads((DASHBOARD_DATA / "dashboard-data.json").read_text())
    b = data["bounds"]
    mlat = (b["maxLat"] - b["minLat"]) * 0.06
    mlon = (b["maxLon"] - b["minLon"]) * 0.06
    return f"{b['minLat'] - mlat},{b['minLon'] - mlon},{b['maxLat'] + mlat},{b['maxLon'] + mlon}"


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--bbox", help="south,west,north,east (defaults to route bounds + margin)")
    args = ap.parse_args()

    bbox = args.bbox or default_bbox()
    print(f"bbox: {bbox}")

    print("· water …")
    water = fetch_water(bbox)
    print(f"  {len(water)} polygons")
    print("· rivers …")
    rivers = fetch_rivers(bbox)
    print(f"  {len(rivers)} lines")
    print("· roads …")
    roads = fetch_roads(bbox)
    print(f"  {len(roads['highway'])} highway, {len(roads['arterial'])} arterial")
    print("· boundaries …")
    boundary = fetch_boundaries(bbox)
    print(f"  {len(boundary['region'])} region ways, {len(boundary['municipal'])} municipal ways")

    s, w, n, e = (float(x) for x in bbox.split(","))
    out = {
        "generatedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "source": "OpenStreetMap contributors (ODbL)",
        "bbox": {"minLat": s, "minLon": w, "maxLat": n, "maxLon": e},
        "water": water,
        "rivers": rivers,
        "roads": roads,
        "boundary": boundary,
    }
    OUT_PATH.write_text(json.dumps(out, separators=(",", ":")))
    kb = OUT_PATH.stat().st_size / 1024
    print(f"\nwrote {OUT_PATH.relative_to(REPO)} ({kb:.0f} KB)")


if __name__ == "__main__":
    main()
