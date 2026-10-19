"""Draws query parameters where a scenario's data is (2026-10-19), for the generateParameterVariations.py scripts.

The files in data_extent/ (written by sample_data_extent.py from the loaded tables) hold, per scenario:
  <scenario>_sample.csv   4,000 points drawn at random from the point table (lon, lat, timestamp)
  <scenario>_pairs.csv    500 pairs of points of the same id (trip, crossing, flight, user)
  <place file>            the places (harbours, islands, cities, universities) with points near them, each with
                          up to 20 timestamps of such points
With them:
  - a point is one of the sampled points;
  - a region (rectangle) and a time window keep their size, but are placed so that they contain a sampled
    point / timestamp, at a random position inside them;
  - an instant is a sampled timestamp;
  - a place is drawn only from the places with data;
and the parameters of one query that belong together are drawn together (begin() before each parameter set):
  - a place and a time window or instant: the window contains / the instant is a timestamp of a point near the place;
  - two regions (region, second_region) and a time window: the regions contain the two points of a pair, the
    window both their timestamps;
  - ANCHOR_HOURS: a query that only counts certain hours of the day gets a window around a timestamp in them.
Each generator calls use(<scenario>) first. Without the files every function returns None and the
generators draw uniformly over the scenario's extent, as before."""
import csv
import os
from datetime import datetime, timedelta

DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data_extent")
PLACES = {"ais": {"harbours": "ais_harbours.csv", "islands": "ais_islands.csv"},
          "aviation": {"cities": "aviation_cities.csv"},
          "cycling": {"universities": "cycling_universities.csv"}}
PLACE_PARAMS = {"harbour": "harbours", "port": "harbours", "island": "islands", "city": "cities",
                "university": "universities"}
ANCHOR_HOURS = {"RidesDuringRushHourInPeriod": {7, 8, 9, 16, 17, 18}}
FMT = "%Y-%m-%d %H:%M:%S"
_points, _pairs, _places, _ctx = [], [], {}, {}


def _rows(name):
    p = os.path.join(DIR, name)
    if not os.path.exists(p):
        return None
    with open(p, newline="", encoding="utf-8") as f:
        return list(csv.reader(f))[1:]


def use(scenario):
    global _points, _pairs, _places
    _points = [(float(r[0]), float(r[1]), datetime.strptime(r[2], FMT)) for r in _rows(f"{scenario}_sample.csv") or []]
    _pairs = [((float(r[0]), float(r[1]), datetime.strptime(r[2], FMT)), (float(r[3]), float(r[4]), datetime.strptime(r[5], FMT)))
              for r in _rows(f"{scenario}_pairs.csv") or []]
    _places = {}
    for kind, name in PLACES.get(scenario, {}).items():
        rows = _rows(name)
        if rows is not None:
            _places[kind] = {r[0]: [datetime.strptime(t, FMT) for t in (r[1].split(";") if len(r) > 1 and r[1] else [])]
                             for r in rows}
    _ctx.clear()


def begin(rnd, params, query=None):
    """Called before each parameter set of a query: draws what several of its parameters must share."""
    _ctx.clear()
    if query in ANCHOR_HOURS:
        _ctx["hours"] = ANCHOR_HOURS[query]
    for p in params or []:
        kind = PLACE_PARAMS.get(p)
        if kind in _places:
            keys = sorted(k for k, ts in _places[kind].items() if ts)
            if keys:
                key = rnd.choice(keys)
                _ctx["place"], _ctx["times"] = (kind, key), _places[kind][key]
            break
    if params and "region" in params and "second_region" in params and _pairs:
        a, b = rnd.choice(_pairs)
        _ctx["points"], _ctx["cover"] = [a[:2], b[:2]], [a[2], b[2]]


def lonlat(rnd):
    """A sampled point's (lon, lat), or None."""
    if _ctx.get("points"):
        return _ctx["points"].pop(0)
    if not _points:
        return None
    lon, lat, _ = rnd.choice(_points)
    return lon, lat


def _times():
    ts = _ctx.get("times") or [t for _, _, t in _points]
    hours = _ctx.get("hours")
    if hours:
        ts = [t for t in ts if t.hour in hours] or ts
    return ts


def instant(rnd):
    """A sampled timestamp (datetime), or None."""
    ts = _times()
    return rnd.choice(ts) if ts else None


def rect_start(rnd, min_lon, min_lat, max_lon, max_lat, width, height):
    """Lower-left corner (lon, lat) of a width x height rectangle within the bounds that contains a sampled
    point, or None."""
    if _ctx.get("points"):
        inside = [_ctx["points"].pop(0)]
    else:
        inside = [(lon, lat) for lon, lat, _ in _points if min_lon <= lon <= max_lon and min_lat <= lat <= max_lat]
    if not inside:
        return None
    lon, lat = rnd.choice(inside)
    x = min(max(lon - rnd.random() * width, min_lon), max_lon - width)
    y = min(max(lat - rnd.random() * height, min_lat), max_lat - height)
    return x, y


def window_start(rnd, lower, upper, seconds):
    """Start (datetime) of a time window of the given length within [lower, upper] that contains a sampled
    timestamp (or both timestamps of the pair drawn by begin()), or None."""
    span = timedelta(seconds=seconds)
    cover = _ctx.get("cover")
    if cover and max(cover) - min(cover) <= span:
        start = max(cover) - span + (min(cover) - (max(cover) - span)) * rnd.random()
    else:
        ts = [t for t in _times() if lower <= t <= upper]
        if not ts:
            return None
        start = rnd.choice(ts) - span * rnd.random()
    return min(max(start, lower), upper - span)


def allowed(kind):
    """The places of this kind with data near them (a set of names/ids as strings), or None."""
    return set(_places[kind]) if kind in _places else None


def place(kind):
    """The place begin() drew for this query's parameter set, or None."""
    p = _ctx.get("place")
    return p[1] if p and p[0] == kind else None
