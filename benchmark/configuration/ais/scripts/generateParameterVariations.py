import random
from datetime import datetime, timedelta
from typing import List, Any, Optional
import csv
import os
import yaml
from dataclasses import dataclass
import re
import psycopg2
import shutil
import math
from typing import Tuple
import pyarrow.parquet as pq
import sys as _sys
_sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "scripts"))
import spacetime_units  # noqa: E402  (shared EPSG:3857 -> SpaceTime radius conversion)
import sedona_crs  # noqa: E402

class ScratchIdPool:
    """
    Mints ids for write-benchmark rows (insert/update/delete/bulk_insert) from a negative id
    space, so they're guaranteed disjoint from every dataset's real (non-negative) ids no matter
    how large real data grows -- the previous fixed positive band (1_000_000_000-2_000_000_000)
    already collided with real data (crossing_id on the live cluster runs up to ~13.05B). Negative
    ids also make cleanup between repeated experiments a single cheap, targeted
    "DELETE ... WHERE id < 0" per table instead of a full reload.

    update/delete draw from ids this same run already minted via insert/bulk_insert, so they
    exercise a real row instead of (almost always) matching nothing. Best-effort: a pipeline that
    reshuffles the full task list after generation (e.g. ais's index shuffle in __main__) can still
    place a delete before its paired insert; that's a harmless no-op, and the negative-id purge
    cleans up regardless of execution order.

    Each live entry is an (id, timestamp) pair, not just an id: crossing_points has no per-row
    primary key, so "id" alone identifies a crossing (every point of it), not a single point. The
    *_single update/delete queries need the exact timestamp their scratch row was inserted with
    too, so pick_existing_pair/pop_existing_pair hand back both -- see updateCrossingPointSingle/
    deleteCrossingPointSingle in combinedBenchConf.yaml.
    """
    def __init__(self):
        self._next_id = -1
        self._live = []  # list of [id, timestamp] pairs

    def mint(self, ts=None):
        id_val = self._next_id
        self._next_id -= 1
        self._live.append([id_val, ts])
        return id_val

    def pick_existing(self, rnd):
        return rnd.choice(self._live)[0] if self._live else self.mint()

    def pop_existing(self, rnd):
        if not self._live:
            return self.mint()
        return self._live.pop(rnd.randrange(len(self._live)))[0]

    def pick_existing_pair(self, rnd):
        if not self._live:
            return self.mint(), None
        return tuple(rnd.choice(self._live))

    def pop_existing_pair(self, rnd):
        if not self._live:
            return self.mint(), None
        return tuple(self._live.pop(rnd.randrange(len(self._live))))


_SCRATCH_IDS = ScratchIdPool()


def get_scratch_id(rnd, query_type):
    # update_single/delete_single normally resolve id+instant together as a pair before this is
    # ever called (see return_param_values) -- these two branches are just a defensive fallback
    # for a *_single query that (misconfigured) omits "instant" from its parameters.
    if query_type in ("update", "update_single"):
        return _SCRATCH_IDS.pick_existing(rnd)
    if query_type in ("delete", "delete_single"):
        return _SCRATCH_IDS.pop_existing(rnd)
    return _SCRATCH_IDS.mint()

def generate_bulk_values(rnd, bbox, lower, upper, batch_size=10):
    # Returns (postgis_rows, clickhouse_rows): the same id/timestamp/location per row, formatted
    # for each dialect's real columns -- crossing_points has "geom" in postgis/mobilitydb/duckdb
    # but only plain "lat"/"lon" columns in ClickHouse (see insertSingleCrossingPoint's FIX note
    # in combinedBenchConf.yaml).
    postgis_rows = []
    clickhouse_rows = []
    for _ in range(batch_size):
        lon, lat = get_random_lonlat(rnd, bbox)
        ts = generate_random_timestamp_in_bounds(rnd, lower, upper)
        id_val = _SCRATCH_IDS.mint(ts)
        postgis_rows.append(f"({id_val}, ST_SetSRID(ST_Point({lon}, {lat}), 3857), TIMESTAMP '{ts}')")
        clickhouse_rows.append(f"({id_val}, TIMESTAMP '{ts}', {lat}, {lon})")
    #return without single quotes around the values, so that it can be used directly in the SQL statement
    return ", ".join(postgis_rows), ", ".join(clickhouse_rows)

# def get_db_connection():
#     return psycopg2.connect(
#         host="localhost",
#         port=5433,
#         dbname="postgres",
#         user="postgres",
#         password="test"
#     )
def get_time_bounds(table, time_col="timestamp_col"):
    if os.path.exists("time_bounds.txt"):
        with open("time_bounds.txt", "r") as f:
            bounds = f.read().strip().split(",")
            lower = datetime.strptime(bounds[0], "%Y-%m-%d %H:%M:%S")
            upper = datetime.strptime(bounds[1], "%Y-%m-%d %H:%M:%S")
            return lower, upper
    # with conn.cursor() as cur:
    #     cur.execute(f"SELECT MIN({time_col}), MAX({time_col}) FROM {table};")
    #     lower, upper = cur.fetchone()
    with open("time_bounds.txt", "w") as f:
        f.write(f"{lower},{upper}")
    return lower, upper

# --- YAML helper for multi-line SQL ---
class LiteralString(str): pass
def literal_str_representer(dumper, data):
    return dumper.represent_scalar('tag:yaml.org,2002:str', data, style='|')
yaml.add_representer(LiteralString, literal_str_representer)

# --- QueryTask structure ---
@dataclass
class QueryTask:
    name: str
    type: str
    sql: str
    params: list


def generate_random_region_bbox(
    rnd,
    bbox: Tuple[float, float, float, float],
    coverage: float
) -> Tuple[float, float, float, float]:
    """
    Generate a random bbox covering approximately `coverage` fraction
    of the original bbox area.

    bbox format:
        (min_lat, min_lon, max_lat, max_lon)

    coverage:
        0.02 -> 2%
        0.05 -> 5%
        0.08 -> 8%
    """
    
    min_lat, min_lon = bbox[0]
    max_lat, max_lon = bbox[1]

    lat_span = max_lat - min_lat
    lon_span = max_lon - min_lon

    # Scale dimensions so width*height = coverage of original area
    scale = math.sqrt(coverage)

    new_lat_span = lat_span * scale
    new_lon_span = lon_span * scale

    # Random lower-left corner
    start_lat = rnd.uniform(
        min_lat,
        max_lat - new_lat_span
    )

    start_lon = rnd.uniform(
        min_lon,
        max_lon - new_lon_span
    )

    return (f"ST_Transform(ST_SetSRID(ST_MakeEnvelope({start_lon}, {start_lat}, {start_lon + new_lon_span}, {start_lat + new_lat_span}), 4326), 3857)"
    )

# --- Config loading ---
def load_config(path="../config/combinedBenchConf.yaml"):
    try:
        with open(path) as f:
            return yaml.safe_load(f)
    except Exception as e:
        print(f"Failed to load config: {e}")
        return None

def substitute_outside_comments(text: str, token: str, replacement: str) -> str:
    """
    Like text.replace(token, replacement), but skips "-- " comment lines. Several FIX comments
    in combinedBenchConf.yaml mention parameter names like ":point_lon" as plain prose explaining
    a bug -- a blind whole-string replace would substitute those mentions too, turning readable
    documentation into "assuming decomposed 50.74 / 6.70 parameters". Substitution should only
    ever touch the actual SQL.
    """
    lines = text.split("\n")
    for i, line in enumerate(lines):
        if not line.strip().startswith("--"):
            lines[i] = line.replace(token, replacement)
    return "\n".join(lines)


# --- Parameter substitution ---
def strip_comment_lines(text: str) -> str:
    """
    Removes "-- " comment lines from a fully-substituted query before it's written to the
    generated queries/*.yaml files. Those files are consumed directly by the Java benchmark
    client to execute against a real database -- they're a compiled artifact, not documentation,
    so the developer-facing FIX/NOT IMPLEMENTED/etc. comments that belong in
    combinedBenchConf.yaml (for readability there) have no reason to also ship in every generated
    query. This runs after substitution, so it operates on the same lines
    substitute_outside_comments() was told to leave alone.
    """
    lines = [l for l in text.split("\n") if not l.strip().startswith("--")]
    return "\n".join(lines)



def duckdb_spatial_literal(value):
    """FIX (2026-10-01): :point/:region values are rendered in PostGIS syntax
    (ST_Transform(ST_SetSRID(..., 4326), 3857)), but DuckDB's spatial extension has no ST_SetSRID
    and its ST_Transform takes explicit CRS strings -- every DuckDB query using one of these params
    failed with "st_setsrid does not exist". always_xy keeps lon/lat axis order (verified: output
    matches PostGIS's ST_Transform exactly)."""
    if not isinstance(value, str):
        return value
    value = re.sub(r"ST_Transform\(ST_SetSRID\((ST_(?:Point|MakeEnvelope)\([^()]*\)),\s*4326\),\s*3857\)",
                   r"ST_Transform(\1, 'EPSG:4326', 'EPSG:3857', true)", value)
    return re.sub(r"ST_SetSRID\((ST_Point\([^()]*\)),\s*\d+\)", r"\1", value)


_WRITE_TYPES = {"insert", "update", "update_single", "delete", "delete_single", "bulk_insert"}

def mobilitydb_utc_literals(sql):
    """FIX (2026-10-01): the trajectory tables hold the point tables' naive wall-clock digits as UTC
    instants (see postgis_ais/postgis_aviation's build), so an offset-less tstzspan/timestamptz
    literal, which Postgres reads in the *session* zone (Europe/Berlin), was 1-2h off against them.
    Pin each literal to +00 so the queries don't depend on the session time zone at all."""
    def pin(m):
        body = re.sub(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:\.\d+)?)(?![\d.]*\s*[+-]\d)", r"\1+00", m.group(2))
        return f"{m.group(1)} '[{body}]'"
    return re.sub(r"(tstzspan|timestamptz) '\[([^\]']+)\]'", pin, sql)

def return_param_values(bbox, lower, upper, sql1, sql2, sql3, sql4, sql5, sql6, params, rnd, query_type) -> List[Any]:
    parsedSQL1, parsedSQL2, parsedSQL3, parsedSQL4 = sql1, sql2, sql3, sql4
    parsedSQL5, parsedSQL6 = sql5, sql6  # clickhouse, duckdb
    values = []

    # "id" alone doesn't identify a single point (see ScratchIdPool's docstring) -- update_single/
    # delete_single queries also declare "instant" and need it to be the *exact* timestamp their
    # target row was inserted with, not an independently-generated random one, or "WHERE id = :id
    # AND timestamp = :instant" would almost always match nothing. Resolve id+instant together as
    # a pair here, before the per-param loop below, so it doesn't matter which order "id"/"instant"
    # appear in this query's parameters: list. Plain insert pairs a freshly minted id with the
    # instant it's about to be written with, so a later update_single/delete_single can find it.
    shared_id = None
    shared_instant = None
    if "id" in params and "instant" in params:
        if query_type in ("update_single", "delete_single"):
            picker = _SCRATCH_IDS.pick_existing_pair if query_type == "update_single" else _SCRATCH_IDS.pop_existing_pair
            shared_id, shared_instant = picker(rnd)
        elif query_type == "insert":
            shared_instant = generate_random_timestamp_in_bounds(rnd, lower, upper)
            shared_id = _SCRATCH_IDS.mint(shared_instant)

    for param in params:
        replacement = {
            "period_short": lambda: generate_random_time_span_in_bounds(rnd, lower, upper, mode=1),
            "period_medium": lambda: generate_random_time_span_in_bounds(rnd, lower, upper, mode=2),
            "period_long": lambda: generate_random_time_span_in_bounds(rnd, lower, upper, mode=3),
            "period": lambda: generate_random_time_span_in_bounds(rnd, lower, upper),
            # FIX: combinedBenchConf.yaml's busiestDayInWeekPeriod (postgis/sedona/spacetime/
            # clickhouse/duckdb) declares :period_week, but this dict never defined it -- the
            # unmatched .get(param, lambda: "")() fallback silently substituted an empty string,
            # producing "BETWEEN '' AND c.id > 0" (confirmed live: every dialect either errors on
            # an empty range or, worse, never errors and just returns nothing for the whole day).
            # Reusing mode=2 (2-15 days) as the closest existing bucket to "a week" rather than
            # inventing a new mode; no other scenario's generator has a period_week concept to
            # match against.
            "period_week": lambda: generate_random_time_span_in_bounds(rnd, lower, upper, mode=2),
            "instant": lambda: shared_instant if shared_instant is not None else generate_random_timestamp_in_bounds(rnd, lower, upper),
            "day": lambda: get_random_day(rnd, lower, upper),
            "hour": lambda: get_random_hour(rnd),
            "harbour": lambda: get_random_place("harbours", rnd),
            "region_medium": lambda: generate_random_region_bbox(rnd, bbox, coverage=0.05),
            "region_medium2": lambda: generate_random_region_bbox(rnd, bbox, coverage=0.05),
            "port": lambda: get_random_place("harbours", rnd),
            "island": lambda: get_random_island_id(rnd),
            "region": lambda: get_random_place("regions-wkt-simplified", rnd, ";"),
            "point": lambda: get_random_point(rnd, bbox),
            "radius": lambda: rnd.randint(5000,15000),  # in meters, harbour queries (2026-10-18, was 1000-5000)
            "distance": lambda: rnd.randint(1000,5000), #in meters
            "id": lambda: shared_id if shared_id is not None else get_scratch_id(rnd, query_type),
            "bulk_values": lambda: generate_bulk_values(rnd, bbox, lower, upper)
        }.get(param, lambda: "")()


        if replacement is not None:
            if(param == "bulk_values"):
                # generate_bulk_values now returns (postgis_rows, clickhouse_rows) -- see its own
                # comment. `quoted` (used for :bulk_values, shared by postgis/mobilitydb/duckdb/
                # sedona/spacetime) stays the postgis-shaped rows; :bulk_values_clickhouse is
                # substituted directly below rather than derived from `quoted` by regex.
                quoted = replacement[0]
                parsedSQL5 = substitute_outside_comments(parsedSQL5, ":bulk_values_clickhouse", replacement[1])
            elif isinstance(replacement, str):
                # Detect SQL expressions like ST_*, SELECT, or functions with parentheses
                if replacement.strip().upper().startswith(("ST_", "SELECT", "POINT(", "POLYGON(", "LINESTRING(")):
                    quoted = replacement  # leave unquoted (SQL expression)
                else:
                    # (2026-10-09) names such as the harbour "'t Waar" contain a quote; doubled, as SQL escapes it
                    quoted = "'" + replacement.replace("'", "''") + "'"
            # if param is bulk_values, we want to insert the raw string without quotes
            
            elif isinstance(replacement, list):
                # Keep your old list quoting (used for time spans)
                quoted = ", ".join([f"'{ts}'" for ts in replacement])
                quoted = f"[{quoted}]"
            else:
                quoted = str(replacement)


            # ClickHouse has no PostGIS-style point/geometry type, so a couple of queries
            # decompose ":point" into ":point_lon"/":point_lat" scalars for that dialect only
            # (see the FIX comments on those clickhouse: blocks in combinedBenchConf.yaml).
            # This must run before the generic ":point" replace below -- str.replace(":point", ...)
            # would otherwise also match the ":point" prefix inside ":point_lon"/":point_lat".
            if param == "point" and isinstance(replacement, str):
                point_coords = re.search(r"ST_Point\(\s*([^,]+?)\s*,\s*([^)]+?)\s*\)", replacement)
                if point_coords:
                    parsedSQL5 = substitute_outside_comments(parsedSQL5, ":point_lon", point_coords.group(1))
                    parsedSQL5 = substitute_outside_comments(parsedSQL5, ":point_lat", point_coords.group(2))

            # FIX: ClickHouse (and, less certainly, DuckDB) have no OVERLAPS operator -- queries
            # that need "(start, end) OVERLAPS :period_x" have to be rewritten as an explicit
            # "start_time < :period_x_end AND :period_x_start < end_time" boolean instead, which
            # needs the period's two bounds as separate scalars. :period_x itself only ever
            # arrives as one opaque packed value (a bracket-list here -- see
            # generate_random_time_span_in_bounds's list return -- or "'ts1' AND 'ts2'" in
            # aviation/cycling's own copy of this script), so combinedBenchConf.yaml's
            # clickhouse/duckdb blocks couldn't actually use :period_x_start/:period_x_end
            # despite already anticipating that exact naming (see countActiveCrossingsInPeriod's
            # duckdb block, written for these tokens before they existed). Decomposing here,
            # before the generic :{param} substitution below, the same way :point is split into
            # :point_lon/:point_lat above.
            if param.startswith("period"):
                ts1 = ts2 = None
                if isinstance(replacement, list) and len(replacement) == 2:
                    ts1, ts2 = replacement
                elif isinstance(replacement, str):
                    span_match = re.match(r"'([^']+)' AND '([^']+)'", replacement)
                    if span_match:
                        ts1, ts2 = span_match.group(1), span_match.group(2)
                if ts1 is not None and ts2 is not None:
                    # (2026-10-02) every dialect, not just clickhouse/duckdb: the trip-span queries
                    # on the *_summary tables need the two bounds separately everywhere
                    parsedSQL1, parsedSQL2, parsedSQL3, parsedSQL4, parsedSQL5, parsedSQL6 = [
                        substitute_outside_comments(substitute_outside_comments(q, f":{param}_start", f"'{ts1}'"),
                                                    f":{param}_end", f"'{ts2}'")
                        for q in (parsedSQL1, parsedSQL2, parsedSQL3, parsedSQL4, parsedSQL5, parsedSQL6)]

            # FIX: replacement is now a (postgis_rows, clickhouse_rows) tuple for "bulk_values" --
            # PyYAML would serialize a bare tuple with its own "!!python/tuple" tag, which Java's
            # SnakeYAML (the client's actual YAML reader) doesn't recognize (confirmed live for the
            # equivalent aviation/scripts case). params: is only ever informational documentation
            # of what was substituted (not read back by the client), so the postgis-shaped half is
            # fine here.
            values.append(quoted if param == "bulk_values" else replacement)
            # FIX: generate_random_time_span_in_bounds() returns "'ts1' AND 'ts2'" -- a fragment
            # meant to slot directly after postgis/sedona/clickhouse/duckdb's "BETWEEN :period_x".
            # MobilityDB's templates use the same :period_x as an operand to && (overlap) or
            # inside attime()/atTime(), which needs a proper tstzspan literal instead -- the
            # generic `quoted` value (which just wraps the whole "'ts1' AND 'ts2'" fragment in one
            # more layer of quotes) produces invalid SQL there ("syntax error at or near '['" once
            # the mobilitydb-only regex further below mangles the embedded dates). Substitute
            # mobilitydb's own copy with a real tstzspan literal built from the same random draw,
            # before the shared substitution below runs for the other five dialects unchanged
            # (same fix as aviation/scripts/generateParameterVariations.py).
            if param.startswith("period") and isinstance(replacement, str):
                # ais's own generate_random_time_span_in_bounds() returns a *list* [ts1, ts2]
                # (unlike aviation/cycling's string-returning version) -- that already produces
                # the bracket-list `quoted` shape the pattern/convert_to_tstzspan regex further
                # below expects and correctly handles, so only the string-returning case needs
                # this dedicated branch at all.
                span_match = re.match(r"'([^']+)' AND '([^']+)'", replacement)
                if span_match:
                    parsedSQL1 = substitute_outside_comments(
                        parsedSQL1, f":{param}",
                        f"tstzspan '[{span_match.group(1)}, {span_match.group(2)}]'"
                    )
                else:
                    parsedSQL1 = substitute_outside_comments(parsedSQL1, f":{param}", quoted)
            else:
                parsedSQL1 = substitute_outside_comments(parsedSQL1, f":{param}", quoted)
            parsedSQL2 = substitute_outside_comments(parsedSQL2, f":{param}", quoted)
            parsedSQL3 = substitute_outside_comments(parsedSQL3, f":{param}", quoted)
            if param.startswith("period"):
                # FIX: confirmed live against the SpaceTime engine -- EPOCHRANGE takes a single
                # bracketed range literal ("EPOCHRANGE '[t1, t2]'"), not the "'t1' AND 't2'"
                # fragment built for the other five dialects' "BETWEEN :period_x", nor the
                # per-element-quoted "['t1', 't2']" the generic `quoted` list-branch produces for
                # this dataset's own list-returning generate_random_time_span_in_bounds()
                # (EPOCHRANGE('t1','t2') -> "unknown function: epochrange"; EPOCHRANGE 't1' AND
                # 't2' -> "no matching overload for: and"; the bracketed form succeeds).
                ts_pair = None
                if isinstance(replacement, str):
                    span_match = re.match(r"'([^']+)' AND '([^']+)'", replacement)
                    if span_match:
                        ts_pair = (span_match.group(1), span_match.group(2))
                elif isinstance(replacement, list) and len(replacement) == 2:
                    ts_pair = (replacement[0], replacement[1])
                if ts_pair:
                    parsedSQL4 = substitute_outside_comments(
                        parsedSQL4, f":{param}", f"'[{ts_pair[0]}, {ts_pair[1]}]'"
                    )
                else:
                    parsedSQL4 = substitute_outside_comments(parsedSQL4, f":{param}", quoted)
            elif param == "instant":
                # FIX: confirmed live -- a bare quoted timestamp in SpaceTime arithmetic (e.g.
                # CityActivityAtInstant's "(:instant - TP '10s')") fails ("no matching overload
                # for: -"); needs the same explicit type prefix TP already needs for durations
                # ("TS 'ts' - TP '10s'" succeeds).
                parsedSQL4 = substitute_outside_comments(parsedSQL4, f":{param}", f"TS {quoted}")
            elif param == "island":
                # FIX (2026-10-02): SpaceTime numbers our islands_coastlines ids +1 and splits them
                # over two tables -- island polygons 1..43 in "islands" (ours 0..42), coastline
                # linestrings 44..97 in "coastline" (ours 43..96) -- verified one-to-one by geometry.
                parsedSQL4 = substitute_outside_comments(parsedSQL4, f":{param}", str(replacement + 1))
                if replacement > 42:
                    parsedSQL4 = (parsedSQL4.replace("JOIN islands i", "JOIN coastline i")
                                            .replace("i.geom::gpolygon", "i.geom::glinestring"))
            else:
                parsedSQL4 = substitute_outside_comments(parsedSQL4, f":{param}", quoted)
            parsedSQL5 = substitute_outside_comments(parsedSQL5, f":{param}", quoted)
            parsedSQL6 = substitute_outside_comments(parsedSQL6, f":{param}", duckdb_spatial_literal(quoted))

    return parsedSQL1, parsedSQL2, parsedSQL3, parsedSQL4, parsedSQL5, parsedSQL6, values

# --- Helpers ---

def adjust_timestamp_format_sedona(ts: str) -> str:
    return re.sub(
            r"TIMESTAMP\s*\(\s*'([^']+)'\s*\)",
            r"TIMESTAMP '\1'",
            ts
        )

def get_random_day(rnd, lower,upper):
    #get random day between lower and upper timestamp
    delta = (upper - lower).days
    day_offset = rnd.randint(0, delta)
    random_day = lower + timedelta(days=day_offset)
    return random_day.strftime("%Y-%m-%d")

def get_random_hour(rnd): return rnd.randint(0, 23)


def generate_random_timestamp_in_bounds(rnd, lower, upper):
    delta = (upper - lower).total_seconds()
    ts = lower + timedelta(seconds=rnd.uniform(0, delta))
    return ts.strftime("%Y-%m-%d %H:%M:%S")

def generate_random_time_span_in_bounds(rnd, lower, upper, mode=0):
    delta = (upper - lower).total_seconds()
    mode_ranges = {
        1: (0, 2*24*60*60),         # short
        2: (2*24*60*60, 15*24*60*60), # medium
        3: (15*24*60*60, delta)       # long
    }
    seconds = min(rnd.uniform(*mode_ranges.get(mode, (0, delta))), delta)
    # (2026-10-18) the window keeps its full length: it starts early enough to end within the data
    # (before, a window running past either end was cut at it, down to a few minutes)
    start = lower + timedelta(seconds=rnd.uniform(0, delta - seconds))
    end = start + timedelta(seconds=seconds)
    return [
        start.strftime("%Y-%m-%d %H:%M:%S"),
        end.strftime("%Y-%m-%d %H:%M:%S")
    ]

# --- Spatial bounds ---
def get_spatial_bounds(table, geom_col="geom"):
    #check if file exists with the spatial bounds, if yes, read from file and return
    if os.path.exists("spatial_bounds.txt"):
        with open("spatial_bounds.txt", "r") as f:
            bounds = f.read().strip().split(",")
            return [[float(bounds[0]), float(bounds[3])], [float(bounds[2]), float(bounds[1])]]
    # with conn.cursor() as cur:
    #     cur.execute(f"""
    #         SELECT
    #             ST_XMin(env),
    #             ST_YMin(env),
    #             ST_XMax(env),
    #             ST_YMax(env)
    #         FROM (
    #             SELECT ST_Envelope(ST_Extent({geom_col}::geometry)) AS env
    #             FROM {table}
    #         ) AS sub;
    #     """)
    #     xmin, ymin, xmax, ymax = cur.fetchone()
    # same format as before: [[ul_lon, ul_lat], [br_lon, br_lat]]
    # save the spatial bounds to a file and check the file to prevent executing the request in the future
    # with open("spatial_bounds.txt", "w") as f:
    #     f.write(f"{xmin},{ymin},{xmax},{ymax}")
    # return [[xmin, ymax], [xmax, ymin]]

def get_random_lonlat(rnd, rect):
    ul_lon, ul_lat = rect[0]
    br_lon, br_lat = rect[1]
    lon = ul_lon + rnd.random() * (br_lon - ul_lon)
    lat = br_lat + rnd.random() * (ul_lat - br_lat)
    return lon, lat

def get_random_point(rnd, rect):
    lon, lat = get_random_lonlat(rnd, rect)
    return f"ST_SetSRID(ST_Point({lon}, {lat}), 3857)"

def get_random_place(filename, rnd, delim=','):
    filepath = os.path.join("..", "data", f"{filename}.csv")
    try:
        with open(filepath, newline='', encoding='utf-8') as csvfile:
            reader = list(csv.reader(csvfile, delimiter=delim))
            if len(reader) <= 1: return None
            chosen_row = rnd.choice(reader[1:])
            return chosen_row[0]
    except Exception as e:
        print(f"Error reading {filepath}: {e}")
        return None

# FIX: ":island" used to be sourced from islands-wkt-simplified.csv via get_random_place() --
# 46 real Greek island names (Athens/Saronic Gulf, ~23E/38N) with no relationship at all to this
# project's actual Danish/North Sea AIS region. combinedBenchConf.yaml's own templates (e.g.
# "... JOIN islands i ON ... WHERE i.id = :island ...") always expected :island to be a plain
# integer id, not a name -- but get_random_place() returns a string, which the generic
# substitution logic in return_param_values() then wrapped in quotes (e.g. i.id = 'Patroklos'),
# breaking on the integer id column regardless of which name ended up there. Separately, the
# *loaded* `islands` table (matching supporting_datasets/islands_coastlines.parquet, the actual
# correct source -- Natural Earth minor-island/coastline polygons for this project's real
# region) has no per-feature name at all: its "name" column is just the coarse Natural Earth
# category ("Minor island"/"Coastline"), not a unique identifier -- only "id" (0-96, one row per
# feature) is actually unique. So the fix is two-part: source :island from the real dataset
# (this function), and let it substitute as a bare integer (i.id = 42) matching what the
# template already expects and what the table can actually be joined on -- no name involved.
_ISLAND_IDS_CACHE: Optional[List[int]] = None

def _load_island_ids() -> List[int]:
    global _ISLAND_IDS_CACHE
    if _ISLAND_IDS_CACHE is None:
        parquet_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "..", "..", "..", "..", "server", "multinode", "supporting_datasets",
            "islands_coastlines.parquet",
        )
        table = pq.read_table(parquet_path, columns=["id"])
        _ISLAND_IDS_CACHE = table.column("id").to_pylist()
    return _ISLAND_IDS_CACHE

def get_random_island_id(rnd) -> int:
    """Returns a real `islands.id` (int), picked from supporting_datasets/islands_coastlines.parquet
    -- the same file the `islands` table is actually loaded from. Returned as a plain int (not a
    string) so return_param_values()'s generic substitution leaves it unquoted, matching
    combinedBenchConf.yaml's `i.id = :island` (an integer column, not a name)."""
    return rnd.choice(_load_island_ids())

# --- Regex helpers ---
def convert_to_tstzspan(m): return f"tstzspan '[{m.group(1)}, {m.group(2)}]'"

def convert_to_tsrange(m): return f"tsrange ('{m.group(1)}'::timestamp, '{m.group(2)}'::timestamp)"

def convert_to_timestamptz(match):
    ts = match.group(1)
    # FIX: this used to wrap every bare timestamp literal indiscriminately, causing two
    # distinct bugs found during the 2026-09-29/30 smoke test (see
    # benchmark_client/docs/smoke-test-2026-09-29.md, Fix 1/Fix 2):
    #  1) `<col> = timestamptz '[...]'` (a cast literal compared directly against a column)
    #     trips Citus's "STABLE functions used in UPDATE queries cannot be called with
    #     column references" restriction on this project's distributed+partitioned tables --
    #     confirmed live the identical comparison works fine left as a bare quoted string
    #     relying on implicit coercion. Skipped for exactly this shape.
    #  2) bulk_insert's row-building pass already emits complete `TIMESTAMP '...'` casts;
    #     re-wrapping those here produced `TIMESTAMP timestamptz '[...]'` -- two type
    #     keywords back to back, a syntax error. Skipped when already cast.
    preceding = match.string[:match.start()]
    if re.search(r"=\s*$", preceding) or re.search(r"TIMESTAMP\s*$", preceding, re.IGNORECASE):
        return f"'{ts}'"
    return f"timestamptz '[{ts}]'"
def fix_between_clause(sql): return re.sub(r"BETWEEN\s*\[\s*'([^']+)'\s*,\s*'([^']+)'\s*\]", r"BETWEEN TIMESTAMP('\1') AND TIMESTAMP('\2')", sql)
def adjust_timestamp_format(ts): return re.sub(r"\['([\d\-: ]+)', '([\d\-: ]+)'\]", r"'[\1, \2]'", ts)

def convert_between_format(m): return f"BETWEEN '{m.group(1)}' AND '{m.group(2)}'"
def convert_overlaps_format(match):
    start, end = match.groups()
    return f"OVERLAPS (TIMESTAMP '{start}', TIMESTAMP '{end}')"

def process_file(input_file, output_file):
    # Patterns
    between_pattern = re.compile(
        r"BETWEEN\s*\[\s*'([^']+)'\s*,\s*'([^']+)'\s*\]", re.IGNORECASE
    )
    overlaps_pattern = re.compile(
        r"OVERLAPS\s*\[\s*'([^']+)'\s*,\s*'([^']+)'\s*\]", re.IGNORECASE
    )
    trsange_pattern = re.compile(
        r"tsrange\s*\(\s*'([^']+)'\s*,\s*'([^']+)'\s*\)", re.IGNORECASE
    )

    with open(input_file, "r") as infile, open(output_file, "w") as outfile:
        for line in infile:
            line = between_pattern.sub(convert_between_format, line)
            line = overlaps_pattern.sub(convert_overlaps_format, line)
            
            line = line.replace("[", "tsrange(").replace("]", ")")
            outfile.write(line)
 



# --- Query preparation ---
def prepare_query_tasks(config, rnd):
    mob, pg, sed, st, ch, dd = [], [], [], [], [], []
    lower, upper = get_time_bounds("crossing_points", "timestamp")
    bbox = get_spatial_bounds("crossing_points", "geom")
    for q in config['queryConfigs']:
        if q['use']:
            for _ in range(q['repetition']):
                sql1, sql2, sql3, sql4, sql5, sql6, values = return_param_values(bbox, lower, upper, q['mobilitydb'], q['postgis'], q['sedona'], q['spacetime'], q['clickhouse'], q['duckdb'], q['parameters'], rnd, q['type'])
                sql4 = spacetime_units.apply(sql4, dict(zip(q['parameters'], values)), 56.0)
                sql1 = re.sub(r"\['([\d\-: ]+)', '([\d\-: ]+)'\]", convert_to_tstzspan, sql1)
                sql1 = re.sub(r"'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})'", convert_to_timestamptz, sql1)
                if q['type'] not in _WRITE_TYPES:  # reads go against the "crossings" trajectory table
                    sql1 = mobilitydb_utc_literals(sql1)
                mob.append(QueryTask(q['name'], q['type'], LiteralString(strip_comment_lines(sql1)), values))
                pg.append(QueryTask(q['name'], q['type'], LiteralString(strip_comment_lines(sql2)), values))
                sed.append(QueryTask(q['name'], q['type'], LiteralString(strip_comment_lines(fix_between_clause(sql3))), values))
                st.append(QueryTask(q['name'], q['type'], LiteralString(strip_comment_lines(sql4)), values))
                # ClickHouse and DuckDB use the same Postgres-style BETWEEN/list-bracket
                # substitutions as postgis above -- both get resolved via process_file() too.
                ch.append(QueryTask(q['name'], q['type'], LiteralString(strip_comment_lines(sql5)), values))
                dd.append(QueryTask(q['name'], q['type'], LiteralString(strip_comment_lines(sql6)), values))
    if config['benchmark']['mixed']:
        random.shuffle(sed)
    return mob, pg, sed, st, ch, dd

# --- Main ---
if __name__ == "__main__":
    config = load_config("../config/combinedBenchConf.yaml")
    if not config: exit(1)

    main_random = random.Random(config['benchmark']['random_seed'])
    mob, pg, sed, st, ch, dd = prepare_query_tasks(config, main_random)

    indices = list(range(len(mob)))
    main_random.shuffle(indices)
    mob, pg, sed, st, ch, dd = [[arr[i] for i in indices] for arr in (mob, pg, sed, st, ch, dd)]

    # Write outputs
    with open("../queries/mobilitydb_queries.yaml", "w") as f: yaml.dump([q.__dict__ for q in mob], f, sort_keys=False, allow_unicode=True)

    # (2026-10-02) the *_space/_time_partitioned MobilityDB variants are deprecated (moved to
    # resources/<scenario>/deprecated/, since 2026-10-10 kept locally in
    # single-node/paper/deprecated-queries/, outside git) and no longer generated.

    with open("../queries/postgisSQL_queries_unprocess.yaml", "w") as f: yaml.dump([q.__dict__ for q in pg], f, sort_keys=False, allow_unicode=True)
    process_file("../queries/postgisSQL_queries_unprocess.yaml", "../queries/postgisSQL_queries.yaml")
    with open("../queries/postgisSQL_queries.yaml") as f: 
        content = f.read().replace("crossing_points", "tsdb_crossing_points")
        content = content.replace("crossing_trajectories", "tsdb_crossing_trajectories")
    with open("../queries/tsdb_queries.yaml", "w") as f: f.write(content)
    with open("../queries/sedonaSQL_queries.yaml", "w") as f: yaml.dump([q.__dict__ for q in sed], f, sort_keys=False, allow_unicode=True)
    #change DATE(st.start_ts) to CAST(st.start_ts AS DATE)
    #also change TIMESTAMP('2023-06-04 21:50:50') to TIMESTAMP '2023-06-04 21:50:50'
    with open("../queries/sedonaSQL_queries.yaml") as f:
        content = re.sub(r"DATE\(\s*([\w\.]+)\s*\)", r"CAST(\1 AS DATE)", f.read())
        content = adjust_timestamp_format_sedona(content)
    with open("../queries/sedonaSQL_queries.yaml", "w") as f: f.write(content)
    #also change (cd.start_time, cd.end_time) OVERLAPS ['2019-04-21 05:58:48', '2019-05-05 16:53:26']; to cd.start_time <= TIMESTAMP '2019-05-05 16:53:26' AND cd.end_time >= TIMESTAMP '2019-04-21 05:58:48';
    with open("../queries/sedonaSQL_queries.yaml") as f:
        content = re.sub(r"\(\s*([\w\.]+)\s*,\s*([\w\.]+)\s*\)\s*OVERLAPS\s*\[\s*'([^']+)'\s*,\s*'([^']+)'\s*\]", r"\1 <= TIMESTAMP '\4' AND \2 >= TIMESTAMP '\3'", f.read())
    with open("../queries/sedonaSQL_queries.yaml", "w") as f: f.write(content)
    with open("../queries/spaceTimeSQL_queries.yaml", "w") as f: yaml.dump([q.__dict__ for q in st], f, sort_keys=False, allow_unicode=True)
    with open("../queries/spaceTimeSQL_queries.yaml") as f: content = adjust_timestamp_format(f.read())
    with open("../queries/spaceTimeSQL_queries.yaml", "w") as f: f.write(content)

    # ClickHouse and DuckDB: same unprocess -> process_file pipeline as postgis (both dialects
    # use the same Postgres-style BETWEEN [...]/list-bracket substitution shape).
    with open("../queries/clickhouse_queries_unprocess.yaml", "w") as f: yaml.dump([q.__dict__ for q in ch], f, sort_keys=False, allow_unicode=True)
    process_file("../queries/clickhouse_queries_unprocess.yaml", "../queries/clickhouse_queries.yaml")

    with open("../queries/duckdb_queries_unprocess.yaml", "w") as f: yaml.dump([q.__dict__ for q in dd], f, sort_keys=False, allow_unicode=True)
    process_file("../queries/duckdb_queries_unprocess.yaml", "../queries/duckdb_queries.yaml")

    #copy all created files to benchmark/client/benchmark_client/src/main/resources/ais

    resource_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "..", "client", "benchmark_client", "src", "main", "resources", "ais")
    shutil.copy("../queries/mobilitydb_queries.yaml", os.path.join(resource_path, "mobilitydb_queries.yaml"))
    shutil.copy("../queries/postgisSQL_queries.yaml", os.path.join(resource_path, "postgisSQL_queries.yaml"))
    shutil.copy("../queries/tsdb_queries.yaml", os.path.join(resource_path, "tsdb_queries.yaml"))
    sedona_crs.fix_file("../queries/sedonaSQL_queries.yaml")  # explicit CRS strings for ST_Transform (scripts/sedona_crs.py)
    shutil.copy("../queries/sedonaSQL_queries.yaml", os.path.join(resource_path, "sedonaSQL_queries.yaml"))
    shutil.copy("../queries/spaceTimeSQL_queries.yaml", os.path.join(resource_path, "spaceTimeSQL_queries.yaml"))
    shutil.copy("../queries/clickhouse_queries.yaml", os.path.join(resource_path, "clickhouse_queries.yaml"))
    shutil.copy("../queries/duckdb_queries.yaml", os.path.join(resource_path, "duckdb_queries.yaml"))