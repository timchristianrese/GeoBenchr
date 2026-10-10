"""Distance-unit conversion between the EPSG:3857 dialects and SpaceTime.

PostGIS/MobilityDB/DuckDB/Sedona measure on EPSG:3857 geometries and ClickHouse via
webMercatorDistance, so a radius R is in Web Mercator units: R covers R*cos(lat) true meters.
SpaceTime stores coordinates in Mireo World-Point (Web Mercator in degree units scaled by
2^28/360: wpx = lon*2^28/360, wpy = degrees(ln(tan(pi/4 + lat/2)))*2^28/360), but its
ST_Distance/ST_DWithin return locally scale-corrected meters. Measured live (2026-10-02) at 40, 52
and 58 degrees N, for the same pair of points:

    spacetime_distance = epsg3857_distance * cos(lat) * 0.99842

So the SpaceTime radius equivalent to a 3857 radius R is R * st_scale(lat), and a SpaceTime
distance converts back to 3857 units by dividing by st_scale(lat). Templates write
":radius * :st_scale" (and "... / :st_scale" for returned distances); apply() fills in :st_scale
for the latitude of the query's reference point/object. Over a radius of a few km cos(lat) barely
changes, so one value per query is exact enough.
"""
import csv
import math
import os
import re
import struct

SPACETIME_METER_FACTOR = 0.99842
_HERE = os.path.dirname(os.path.abspath(__file__))
_CONF = os.path.normpath(os.path.join(_HERE, ".."))
_ISLANDS = os.path.normpath(os.path.join(_CONF, "..", "..", "server", "multinode",
                                         "supporting_datasets", "islands_coastlines.parquet"))


def st_scale(lat):
    return math.cos(math.radians(lat)) * SPACETIME_METER_FACTOR


def _mercator_y_to_lat(y):
    return math.degrees(2 * math.atan(math.exp(y / 6378137.0)) - math.pi / 2)


def lat_from_point(value):
    """Latitude of a point parameter, whichever way it's rendered."""
    if not isinstance(value, str):
        return None
    m = re.search(r"ST_GeogPoint\(\s*(-?[\d.]+)\s*,\s*(-?[\d.]+)\s*\)", value) \
        or re.search(r"POINT\s*\(\s*(-?[\d.]+)\s+(-?[\d.]+)\s*\)", value) \
        or re.search(r"ST_Point\(\s*(-?[\d.]+)\s*,\s*(-?[\d.]+)\s*\)", value)
    if not m:
        return None
    x, y = float(m.group(1)), float(m.group(2))
    if abs(x) > 180 or abs(y) > 90:          # EPSG:3857 meters
        return _mercator_y_to_lat(y)
    return y


def _csv_rows(path):
    with open(path, newline="", encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


def _mean_lat(rows, name):
    # universities.csv / berlin-districts.csv label their columns latitude/longitude but store
    # (lon, lat); the latitude is whichever value is the larger one in Berlin (~52 vs ~13).
    lats = []
    for r in rows:
        if r.get("name") == name:
            a, b = float(r["latitude"]), float(r["longitude"])
            lats.append(max(a, b))
    return sum(lats) / len(lats) if lats else None


def _harbour_lat(name):
    for r in _csv_rows(os.path.join(_CONF, "ais", "data", "harbours.csv")):
        if r["name"] == name:
            return float(r["latitude"])
    return None


def _city_lat(name):
    for r in _csv_rows(os.path.join(_CONF, "aviation", "data", "cities.csv")):
        if r["name"] == name:
            return float(r["latitude"])
    return None


_ISLAND_LATS = None


def _wkb_lats(buf, off=0):
    """Return (latitudes, new_offset) of every coordinate in a 2D WKB geometry."""
    endian = "<" if buf[off] == 1 else ">"
    gtype = struct.unpack_from(endian + "I", buf, off + 1)[0] % 1000
    off += 5
    lats = []
    def coords(o):
        n = struct.unpack_from(endian + "I", buf, o)[0]; o += 4
        pts = [struct.unpack_from(endian + "dd", buf, o + 16 * i)[1] for i in range(n)]
        return pts, o + 16 * n
    if gtype == 1:
        lats.append(struct.unpack_from(endian + "dd", buf, off)[1]); off += 16
    elif gtype == 2:
        pts, off = coords(off); lats += pts
    elif gtype == 3:
        nrings = struct.unpack_from(endian + "I", buf, off)[0]; off += 4
        for _ in range(nrings):
            pts, off = coords(off); lats += pts
    elif gtype in (4, 5, 6, 7):
        n = struct.unpack_from(endian + "I", buf, off)[0]; off += 4
        for _ in range(n):
            sub, off = _wkb_lats(buf, off); lats += sub
    return lats, off


def _island_lat(island_id):
    global _ISLAND_LATS
    if _ISLAND_LATS is None:
        import pyarrow.parquet as pq
        t = pq.read_table(_ISLANDS, columns=["id", "geometry"])
        _ISLAND_LATS = {}
        for i, g in zip(t.column("id").to_pylist(), t.column("geometry").to_pylist()):
            lats, _ = _wkb_lats(g)
            _ISLAND_LATS[i] = (min(lats) + max(lats)) / 2 if lats else None
    return _ISLAND_LATS.get(int(island_id))


def reference_lat(params, spacetime_sql, fallback_lat):
    """Latitude to scale this query's radius at: its point parameter, else its reference object."""
    for key in ("point",):
        if key in params:
            lat = lat_from_point(params[key])
            if lat is not None:
                return lat
    lat = lat_from_point(spacetime_sql)
    if lat is not None:
        return lat
    lookups = {
        "harbour": _harbour_lat,
        "island": _island_lat,
        "city": _city_lat,
        "university": lambda n: _mean_lat(_csv_rows(os.path.join(_CONF, "cycling", "data", "universities.csv")), n),
        "district": lambda n: _mean_lat(_csv_rows(os.path.join(_CONF, "cycling", "data", "berlin-districts.csv")), n),
    }
    for key, fn in lookups.items():
        if key in params and params[key] is not None:
            lat = fn(str(params[key]).strip("'"))
            if lat is not None:
                return lat
    return fallback_lat


def apply(spacetime_sql, params, fallback_lat):
    """Fill in :st_scale for this query (no-op if the template doesn't use it)."""
    if not spacetime_sql or ":st_scale" not in spacetime_sql:
        return spacetime_sql
    lat = reference_lat(params, spacetime_sql, fallback_lat)
    return spacetime_sql.replace(":st_scale", f"{st_scale(lat):.6f}")
