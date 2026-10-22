import random
from datetime import datetime, timedelta
from typing import List, Any, Dict, Optional
import csv
import os
import yaml
from dataclasses import dataclass
import yaml
from yaml.representer import SafeRepresenter
import re
import psycopg2
import shutil
from pyproj import Transformer
from typing import Tuple
import sys as _sys
_sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "scripts"))
import spacetime_units  # noqa: E402  (shared EPSG:3857 -> SpaceTime radius conversion)
import data_extent  # noqa: E402  (draws parameters where the data is)
data_extent.use("aviation")
import sedona_crs  # noqa: E402

# FIX: get_random_point() used to build "ST_SetSRID(ST_Point(lon, lat), 3857)" straight from raw
# WGS84 degrees, with no projection at all -- confirmed live against the actual loaded data
# (flight_points.geom, loaded via ST_Transform(ST_SetSRID(ST_MakePoint(lon,lat),4326), 3857) in
# postgis_aviation's real INSERT): real geometry sits at Web Mercator meter-scale coordinates
# like (799571.79, 6810278.30), while the benchmark's own generated "point" params were bare
# degree values like (50.7, 6.19) mislabeled as SRID 3857 -- off by ~5-6 orders of magnitude, so
# ST_DWithin/edwithin/etc. against any real geometry could never meaningfully match. This
# reprojects degrees -> the same Web Mercator meters the real data actually uses.
_TO_WEB_MERCATOR = Transformer.from_crs("EPSG:4326", "EPSG:3857", always_xy=True)

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

    Each live entry is an (id, timestamp) pair, not just an id: flight_points/ride_points/etc. have
    no per-row primary key, so "id" alone identifies a flight (every point of it), not a single
    point. The *_single update/delete queries need the exact timestamp their scratch row was
    inserted with too, so pick_existing_pair/pop_existing_pair hand back both -- see
    updateflightPointSingle/deleteflightPointSingle in combinedBenchConf.yaml.
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
    # Returns (postgis_rows, clickhouse_rows): the same id/timestamp/location/altitude per row,
    # formatted for each dialect's real columns -- see get_random_point's comment for why these
    # need to carry both the projected (meters, for geom-typed columns) and raw (degrees, for
    # ClickHouse's plain lat/lon columns) forms of the same point rather than re-deriving one from
    # the other.
    # FIX: rows didn't include "altitude" -- flight_points has an altitude column (see
    # insertSingleflightPoint's FIX note in combinedBenchConf.yaml) that every row needs.
    postgis_rows = []
    clickhouse_rows = []
    for _ in range(batch_size):
        point_expr, lon, lat = get_random_point(rnd, bbox)
        ts = generate_random_timestamp_in_bounds(rnd, lower, upper)
        altitude = generate_random_altitude(rnd)
        id_val = _SCRATCH_IDS.mint(ts)
        postgis_rows.append(f"({id_val}, {point_expr}, TIMESTAMP '{ts}', {altitude})")
        clickhouse_rows.append(f"({id_val}, TIMESTAMP '{ts}', {lat}, {lon}, {altitude})")
    return ", ".join(postgis_rows), ", ".join(clickhouse_rows)


def convert_st_points_in_yaml(input_path, output_path):
    """
    Converts all instances of ST_Point(x, y)::gpoint from EPSG:3857 (Web Mercator)
    to ST_Point(lon, lat)::gpoint in EPSG:4326 and writes the converted file.
    """
    
    # Transformer: 3857 -> 4326
    transformer = Transformer.from_crs("EPSG:3857", "EPSG:4326", always_xy=True)

    # Regex to find ST_Point(x, y)::gpoint
    pattern = re.compile(
        r"ST_Point\(\s*([+-]?\d+(?:\.\d+)?),\s*([+-]?\d+(?:\.\d+)?)\s*\)::gpoint"
    )

    with open(input_path, "r", encoding="utf-8") as f:
        content = f.read()

    def replace_match(match):
        x = float(match.group(1))
        y = float(match.group(2))

        lon, lat = transformer.transform(x, y)

        # Format with reasonable precision
        return f"ST_GeogPoint({lon:.12f}, {lat:.12f})"

    # Replace all occurrences
    new_content = pattern.sub(replace_match, content)

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(new_content)

    print(f"Converted file written to: {output_path}")
class LiteralString(str): pass


# def get_db_connection():
#     return psycopg2.connect(
#         host="localhost",
#         port=5433,
#         dbname="postgres",
#         user="postgres",
#         password="test"
#     )

def literal_str_representer(dumper, data):
    return dumper.represent_scalar('tag:yaml.org,2002:str', data, style='|')

yaml.add_representer(LiteralString, literal_str_representer)

@dataclass
class QueryTask:
    name: str
    type: str
    sql: str
    params: list

def load_config(path="../config/mobilityDBBenchConf.yaml"):
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

def return_param_values(bbox, lower,upper, sql1,sql2, sql3, sql4, sql5, sql6, params, rnd, query_type) -> List[Any]:
    parsedSQL1 = sql1
    parsedSQL2 = sql2
    parsedSQL3 = sql3
    print("SQL3 before processing:", parsedSQL3)
    parsedSQL4 = sql4
    parsedSQL5 = sql5  # clickhouse
    parsedSQL6 = sql6  # duckdb
    values = []
    formatter = "%Y-%m-%d %H:%M:%S"  # Default formatter for datetime

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
            "instant": lambda: shared_instant if shared_instant is not None else generate_random_timestamp_in_bounds(rnd, lower, upper),
            "duration": lambda: rnd.randint(3600, 6 * 3600),
            "day": lambda: get_random_day(rnd, lower, upper),
            "city": lambda: get_random_place("cities", rnd),
            "municipality": lambda: get_random_place("municipalities", rnd),
            "region_medium": lambda: generate_random_region_bbox(rnd, bbox, coverage=0.05),
            "region_medium2": lambda: generate_random_region_bbox(rnd, bbox, coverage=0.05),
            "county": lambda: get_random_place("counties", rnd),
            "district": lambda: get_random_place("districts", rnd),
            "point": lambda: get_random_point(rnd, bbox),
            "radius": lambda: rnd.randint(1000,5000), #in meters
            "distance": lambda: rnd.randint(1000,5000), #in meters
            # Bounds picked from the actual altitude distribution in
            # data_generators/aviation/parquet_flights (sampled 2 of 676 files, ~8M points): values
            # are ~uniform across 1000-9075 (climb/descent) but over half of all points sit in
            # [9550, 10500] (cruise, observed max), so anchoring both bounds up there gives a band
            # that actually splits the data instead of matching almost everything or almost
            # nothing. Ranges stay disjoint (low tops out at 9600, high starts at 9610) so
            # low_altitude < high_altitude always holds without generating them together; typical
            # draws land in the 500-1300 unit-wide range, matching ~15-50% of points in testing
            # (vs. the previous 1000-6000 range, most of which undershoots the real data entirely).
            "low_altitude": lambda: rnd.randint(880, 960) * 10,
            "high_altitude": lambda: rnd.randint(961, 1050) * 10,
            "altitude": lambda: generate_random_altitude(rnd),
            "id": lambda: shared_id if shared_id is not None else get_scratch_id(rnd, query_type),
            "bulk_values": lambda: generate_bulk_values(rnd, bbox, lower, upper, batch_size=10)
        }.get(param, lambda: "")()

        if replacement is not None:
            if param == "bulk_values":
                # generate_bulk_values now returns (postgis_rows, clickhouse_rows) -- see its own
                # comment. `quoted` (used for :bulk_values, shared by postgis/mobilitydb/duckdb/
                # sedona/spacetime) stays the postgis-shaped rows; :bulk_values_clickhouse is
                # substituted directly below rather than derived from `quoted` by regex.
                quoted = replacement[0]
            elif param == "point":
                # get_random_point now returns (sql_expr, lon, lat) -- see its own comment.
                # `quoted` (used for :point, shared by postgis/mobilitydb/duckdb/sedona/
                # spacetime) stays the projected SQL expression; :point_lon/:point_lat are
                # substituted directly below from the same raw lon/lat this expression was built
                # from, rather than re-parsed out of the (now-projected, meter-scale) expression.
                quoted = replacement[0]
            elif isinstance(replacement, str):
                # Detect SQL expressions like ST_*, SELECT, or functions with parentheses
                if replacement.strip().upper().startswith(("ST_", "SELECT", "POINT(", "POLYGON(", "LINESTRING(")):
                    quoted = replacement  # leave unquoted (SQL expression)
                elif param.startswith("period"):
                    # FIX: generate_random_time_span_in_bounds() already returns a fully-quoted,
                    # ready-to-use "'ts1' AND 'ts2'" fragment for postgis/sedona/clickhouse/
                    # duckdb's "BETWEEN :period_x" -- wrapping it in one more layer of quotes here
                    # produced "''ts1' AND 'ts2''", an unterminated-string syntax error the moment
                    # any dialect actually ran it (confirmed live). Only the bare place names
                    # (city/county/district/...) that also come through this branch need quoting.
                    quoted = replacement
                else:
                    # (2026-10-09) names such as the harbour "'t Waar" contain a quote; doubled, as SQL escapes it
                    quoted = "'" + replacement.replace("'", "''") + "'"
            
            elif isinstance(replacement, list):
                # Keep your old list quoting (used for time spans)
                quoted = ", ".join([f"'{ts}'" for ts in replacement])
                quoted = f"[{quoted}]"
            else:
                quoted = str(replacement)


            # (2026-10-02) split :period_x into :period_x_start/:period_x_end for every dialect
            # (before the generic :{param} substitution -- ":period_x" is a prefix of both), for
            # the trip-span queries on the *_summary tables
            if param.startswith("period"):
                _span = None
                if isinstance(replacement, list) and len(replacement) == 2:
                    _span = (replacement[0], replacement[1])
                elif isinstance(replacement, str):
                    _m = re.match(r"'([^']+)' AND '([^']+)'", replacement)
                    if _m:
                        _span = (_m.group(1), _m.group(2))
                if _span:
                    parsedSQL1, parsedSQL2, parsedSQL3, parsedSQL4, parsedSQL5, parsedSQL6 = [
                        substitute_outside_comments(substitute_outside_comments(q, f":{param}_start", f"'{_span[0]}'"),
                                                    f":{param}_end", f"'{_span[1]}'")
                        for q in (parsedSQL1, parsedSQL2, parsedSQL3, parsedSQL4, parsedSQL5, parsedSQL6)]
            # ClickHouse has no PostGIS-style point/geometry type, so a couple of queries
            # decompose ":point" into ":point_lon"/":point_lat" scalars for that dialect only
            # (see the FIX comments on those clickhouse: blocks in combinedBenchConf.yaml), using
            # the same raw degrees get_random_point already computed (not re-derived from the
            # projected `quoted` expression, which is in meters now -- see its own comment). This
            # MUST run before the generic ":point" replace below -- str.replace(":point", ...)
            # would otherwise also match the ":point" prefix inside ":point_lon"/":point_lat" and
            # mangle them into "<value>_lon"/"<value>_lat".
            if param == "point":
                _, plon, plat = replacement
                parsedSQL5 = substitute_outside_comments(parsedSQL5, ":point_lon", str(plon))
                parsedSQL5 = substitute_outside_comments(parsedSQL5, ":point_lat", str(plat))

            # Same idea as :point above, but for bulk_values: substitute the ClickHouse-shaped
            # rows generate_bulk_values() already built (see its own comment) via a dedicated
            # :bulk_values_clickhouse token, so the query config can opt in per-scenario without
            # disturbing the shared :bulk_values token the other dialects still use.
            if param == "bulk_values":
                parsedSQL5 = substitute_outside_comments(parsedSQL5, ":bulk_values_clickhouse", replacement[1])

            # FIX: replacement is a Python tuple for "point"/"bulk_values" now (see above) -- PyYAML
            # serializes a bare tuple with its own "!!python/tuple" tag, which Java's SnakeYAML
            # (the client's actual YAML reader) doesn't recognize at all: it crashed the manager
            # outright on load ("Global tag is not allowed: tag:yaml.org,2002:python/tuple"),
            # confirmed live. The params: field is only ever informational documentation of what
            # was substituted (not read back by the client), so a plain string repr is fine here.
            values.append(quoted if param in ("point", "bulk_values") else replacement)
            # FIX: generate_random_time_span_in_bounds() returns "'ts1' AND 'ts2'" -- a fragment
            # meant to slot directly after postgis/sedona/clickhouse/duckdb's "BETWEEN :period_x".
            # MobilityDB's templates use the same :period_x as an operand to && (overlap) or
            # inside attime(), which needs a proper tstzspan literal instead -- the generic
            # `quoted` value (which just wraps the whole "'ts1' AND 'ts2'" fragment in one more
            # layer of quotes for the other five dialects) produces invalid SQL there ("syntax
            # error at or near '['" once the now-unrelated pattern_single regex below mangles the
            # embedded dates further). Substitute mobilitydb's own copy with a real tstzspan
            # literal built from the same random draw, before the shared substitution below runs
            # for the other five dialects exactly as it always has.
            if param.startswith("period"):
                span_match = re.match(r"'([^']+)' AND '([^']+)'", replacement)
                if span_match:
                    parsedSQL1 = substitute_outside_comments(
                        parsedSQL1, f":{param}",
                        f"tstzspan '[{span_match.group(1)}, {span_match.group(2)}]'"
                    )
                    # FIX: confirmed live against the SpaceTime engine -- EPOCHRANGE takes a single
                    # bracketed range literal ("EPOCHRANGE '[t1, t2]'"), not the "'t1' AND 't2'"
                    # fragment built for the other five dialects' "BETWEEN :period_x"
                    # (EPOCHRANGE('t1','t2') -> "unknown function: epochrange"; EPOCHRANGE 't1' AND
                    # 't2' -> "no matching overload for: and"; the bracketed form succeeds).
                    parsedSQL4 = substitute_outside_comments(
                        parsedSQL4, f":{param}",
                        f"'[{span_match.group(1)}, {span_match.group(2)}]'"
                    )
                else:
                    parsedSQL1 = substitute_outside_comments(parsedSQL1, f":{param}", quoted)
                    parsedSQL4 = substitute_outside_comments(parsedSQL4, f":{param}", quoted)
            elif param == "instant":
                parsedSQL1 = substitute_outside_comments(parsedSQL1, f":{param}", quoted)
                # FIX: confirmed live -- a bare quoted timestamp in SpaceTime arithmetic (e.g.
                # CityActivityAtInstant's "(:instant - TP '10s')") fails ("no matching overload
                # for: -"); needs the same explicit type prefix TP already needs for durations
                # ("TS 'ts' - TP '10s'" succeeds).
                parsedSQL4 = substitute_outside_comments(parsedSQL4, f":{param}", f"TS {quoted}")
            else:
                parsedSQL1 = substitute_outside_comments(parsedSQL1, f":{param}", quoted)
                parsedSQL4 = substitute_outside_comments(parsedSQL4, f":{param}", quoted)
            parsedSQL2 = substitute_outside_comments(parsedSQL2, f":{param}", quoted)
            parsedSQL3 = substitute_outside_comments(parsedSQL3, f":{param}", quoted)
            parsedSQL5 = substitute_outside_comments(parsedSQL5, f":{param}", quoted)
            parsedSQL6 = substitute_outside_comments(parsedSQL6, f":{param}", duckdb_spatial_literal(quoted))
        # if replacement is not None:
        #     # Quote strings and timestamps
        #     if param == "point" and isinstance(replacement, list) and len(replacement) == 2:
        #     # Wrap in Sedona function call instead of SRID=3857; inside WKT
        #         formatted = f"ST_SetSRID(ST_GeomFromText('POINT({replacement[0]} {replacement[1]})'), 3857)"

        #     elif isinstance(replacement, str):
        #         formatted = f"'{replacement}'"
        #     elif isinstance(replacement, (int, float)):
        #         formatted = str(replacement)
        #     elif isinstance(replacement, list) and all(isinstance(x, str) for x in replacement):
        #         formatted = f"[{', '.join([f'\'{x}\'' for x in replacement])}]"
        #     else:
        #         formatted = str(replacement)

            

            # values.append(replacement)
            # parsedSQL1 = parsedSQL1.replace(f":{param}", formatted)
            # parsedSQL2 = parsedSQL2.replace(f":{param}", formatted)
            # parsedSQL3 = parsedSQL3.replace(f":{param}", formatted)
            # parsedSQL4 = parsedSQL4.replace(f":{param}", formatted)
    return parsedSQL1, parsedSQL2, parsedSQL3, parsedSQL4, parsedSQL5, parsedSQL6, values



def get_random_day(rnd, lower,upper):
    #get random day between lower and upper timestamp
    delta = (upper - lower).days
    day_offset = rnd.randint(0, delta)
    random_day = lower + timedelta(days=day_offset)
    return random_day.day

def generate_random_timestamp(rnd: random.Random) -> str:
    year = 2023
    formatter = "%Y-%m-%d %H:%M:%S"
    start = datetime(year, 1, 1)
    random_seconds = rnd.randint(0, 365 * 24 * 60 * 60 - 1)
    timestamp = start + timedelta(seconds=random_seconds)
    return timestamp.strftime(formatter)

def adjust_timestamp_format(ts: str) -> str:
    """
    Adjust all timestamps in the file from ['2023-07-21 10:08:29', '2023-07-26 23:30:04'] to '[2023-07-21 10:08:29, 2023-07-26 23:30:04]'
    """
    pattern = r"\['([\d\-: ]+)', '([\d\-: ]+)'\]"
    replacement = r"'[\1, \2]'"
    return re.sub(pattern, replacement, ts)

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
    # with open("time_bounds.txt", "w") as f:
    #     f.write(f"{lower},{upper}")
    # return lower, upper

def generate_random_timestamp_in_bounds(rnd, lower, upper):
    delta = (upper - lower).total_seconds()
    ts = data_extent.instant(rnd) or lower + timedelta(seconds=rnd.uniform(0, delta))
    return ts.strftime("%Y-%m-%d %H:%M:%S")

def generate_random_altitude(rnd):
    # Same real-data range noted on low_altitude/high_altitude above (~1000-10500).
    return rnd.randint(1000, 10500)

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
    start = data_extent.window_start(rnd, lower, upper, seconds) or lower + timedelta(seconds=rnd.uniform(0, delta - seconds))
    end = start + timedelta(seconds=seconds)
    return f"'{start.strftime('%Y-%m-%d %H:%M:%S')}' AND '{end.strftime('%Y-%m-%d %H:%M:%S')}'"

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
    
    # (2026-10-19) bbox is [[min_lon, max_lat], [max_lon, min_lat]] (see get_spatial_bounds); this read it as
    # (lat, lon) pairs before, as the general generator once did
    min_lon, max_lat = bbox[0]
    max_lon, min_lat = bbox[1]

    lat_span = max_lat - min_lat
    lon_span = max_lon - min_lon

    # Scale dimensions so width*height = coverage of original area
    scale = math.sqrt(coverage)

    new_lat_span = lat_span * scale
    new_lon_span = lon_span * scale

    # Random lower-left corner
    corner = data_extent.rect_start(rnd, min_lon, min_lat, max_lon, max_lat, new_lon_span, new_lat_span)
    start_lon, start_lat = corner if corner else (rnd.uniform(min_lon, max_lon - new_lon_span),
                                                  rnd.uniform(min_lat, max_lat - new_lat_span))

    return (f"ST_Transform(ST_SetSRID(ST_MakeEnvelope({start_lon}, {start_lat}, {start_lon + new_lon_span}, {start_lat + new_lat_span}), 4326), 3857)"
    )

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
    # # same format as before: [[ul_lon, ul_lat], [br_lon, br_lat]]
    # # save the spatial bounds to a file and check the file to prevent executing the request in the future
    # with open("spatial_bounds.txt", "w") as f:
    #     f.write(f"{xmin},{ymin},{xmax},{ymax}")
    # return [[xmin, ymax], [xmax, ymin]]

def get_random_point(rnd, rect):
    # Returns (sql_expr, lon, lat): sql_expr is ready to drop straight into a query (projected to
    # the same Web Mercator meters the real geom columns use -- see _TO_WEB_MERCATOR's comment);
    # the raw WGS84 degrees are returned alongside for dialects with a plain lat/lon column
    # instead of a geometry type (ClickHouse), which need the un-projected values.
    ul_lon, ul_lat = rect[0]
    br_lon, br_lat = rect[1]
    ll = data_extent.lonlat(rnd)
    lon, lat = ll if ll else (ul_lon + rnd.random() * (br_lon - ul_lon), br_lat + rnd.random() * (ul_lat - br_lat))
    x, y = _TO_WEB_MERCATOR.transform(lon, lat)
    return f"ST_SetSRID(ST_Point({x}, {y}), 3857)", lon, lat

    mode_ranges = {
        1: (0, 2 * 24 * 60 * 60 + 1),
        2: (2 * 24 * 60 * 60, 15 * 24 * 60 * 60 + 1),
        3: (15 * 24 * 60 * 60, 365 * 24 * 60 * 60 + 1)
    }
    if mode in mode_ranges:
        min_shift, max_shift = mode_ranges[mode]
        seconds_to_shift = rnd.randint(min_shift, max_shift)
    else:
        seconds_to_shift = rnd.randint(0, 365 * 24 * 60 * 60)

    shift_direction = 1 if rnd.choice([True, False]) else -1
    tentative_end = timestamp1 + timedelta(seconds=shift_direction * seconds_to_shift)

    # Clamp to same year
    if tentative_end.year != year:
        tentative_end = datetime(year, 12, 31, 23, 59, 59) if tentative_end.year > year else datetime(year, 1, 1, 0, 0, 1)

    start_ts, end_ts = sorted([timestamp1, tentative_end])
    return [start_ts.strftime(formatter), end_ts.strftime(formatter)]

def get_random_place(filename: str, rnd: random.Random) -> Optional[str]:
    pre = data_extent.place(filename)  # the place drawn together with this query's time parameters
    if pre is not None:
        return pre
    filepath = os.path.join("..", "data", f"{filename}.csv")
    
    try:
        with open(filepath, newline='', encoding='utf-8') as csvfile:
            reader = list(csv.reader(csvfile))
            if len(reader) <= 1:
                return None  # No data or only header
            rows = reader[1:]
            ok = data_extent.allowed(filename)
            if ok is not None:
                col = 4 if "cities" in filepath else 0
                rows = [r for r in rows if len(r) > col and r[col] in ok] or rows
            chosen_row = rnd.choice(rows)  # Skip header
            if filepath.__contains__("cities"):
                return chosen_row[4]
            return chosen_row[0]  # Return value from column 0 (name)
    except (FileNotFoundError, IndexError) as e:
        print(f"Error reading {filepath}: {e}")
        return None


def convert_to_tstzspan(match):
    ts1 = match.group(1)
    ts2 = match.group(2)
    return f"tstzspan '[{ts1}, {ts2}]'"



def convert_to_tsrange(match):
    ts1 = match.group(1)
    ts2 = match.group(2)
    return f"tsrange ('{ts1}'::timestamp, '{ts2}'::timestamp)"

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
    #     keywords back to back, a syntax error. Skipped when already cast. (This subsumes
    #     the old negative-lookbehind on pattern_single below -- reverted to a plain pattern
    #     so the fix lives in one place, matching every other scenario's generator script.)
    preceding = match.string[:match.start()]
    if re.search(r"=\s*$", preceding) or re.search(r"TIMESTAMP\s*$", preceding, re.IGNORECASE):
        return f"'{ts}'"
    return f"timestamptz '[{ts}]'"

# def convert_between_format(match):
#     """
#     Regex replacement function to convert BETWEEN format.
#     """
#     timestamp1 = match.group(1).strip()
#     timestamp2 = match.group(2).strip()
    
#     # Validate timestamps (optional)
#     try:
#         datetime.strptime(timestamp1, '%Y-%m-%d %H:%M:%S')
#         datetime.strptime(timestamp2, '%Y-%m-%d %H:%M:%S')
#     except ValueError:
#         # Return original if invalid format
#         return match.group(0)
    
#     return f"BETWEEN '{timestamp1}' AND '{timestamp2}'"

def fix_between_clause(sql_fragment: str) -> str:
    """
    Converts a SQL BETWEEN clause from the format:
    "BETWEEN ['2023-07-21 10:08:29', '2023-07-26 23:30:04']"
    to the format:
    "BETWEEN TIMESTAMP('2023-07-21 10:08:29') AND TIMESTAMP('2023-07-26 23:30:04')"
    """
    pattern = r"BETWEEN\s*\[\s*'([^']+)'\s*,\s*'([^']+)'\s*\]"
    replacement = r"BETWEEN TIMESTAMP('\1') AND TIMESTAMP('\2')"
    return re.sub(pattern, replacement, sql_fragment)






def prepare_query_tasks(config, rnd) -> List[QueryTask]:
    mobilitydb_queries = []
    postgis_queries = []
    sedona_queries = []
    spacetime_queries = []
    clickhouse_queries = []
    duckdb_queries = []

    lower, upper = get_time_bounds("flight_points", "timestamp")
    bbox = get_spatial_bounds("flight_points", "geom")

    for query_config in config['queryConfigs']:
        if query_config['use']:
            for _ in data_extent.repetitions(query_config['repetition']):
                data_extent.begin(rnd, query_config.get("parameters"), query_config.get("name"))
                mobilitydb_sql, postgis_sql, sedona_sql, spacetime_sql, clickhouse_sql, duckdb_sql, values = return_param_values(
                    bbox,
                    lower,
                    upper,
                    query_config['mobilitydb'],
                    query_config['postgis'],
                    query_config['sedona'],
                    query_config['spacetime'],
                    query_config['clickhouse'],
                    query_config['duckdb'],
                    query_config['parameters'],
                    rnd,
                    query_config['type'],
                )
                if not data_extent.accept(values):
                    continue
                spacetime_sql = spacetime_units.apply(spacetime_sql, dict(zip(query_config['parameters'], values)), 51.5)
                pattern = r"\['([\d\-: ]+)', '([\d\-: ]+)'\]"
                # See convert_to_timestamptz()'s FIX comment above -- it now handles the
                # already-cast (bulk_insert) and column-equality cases itself, so this stays a
                # plain pattern (same as every other scenario's generator script).
                pattern_single = r"'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})'"
                mobilitydb_sql = re.sub(pattern, convert_to_tstzspan, mobilitydb_sql)
                mobilitydb_sql = re.sub(pattern_single, convert_to_timestamptz, mobilitydb_sql)
                if query_config['type'] not in _WRITE_TYPES:  # reads go against the "flights" trajectory table
                    mobilitydb_sql = mobilitydb_utc_literals(mobilitydb_sql)
                mobilitydb_queries.append(QueryTask(
                    name=query_config['name'],
                    type=query_config['type'],
                    sql=LiteralString(strip_comment_lines(mobilitydb_sql)), 
                    params=values
                ))

                pattern = r"\['([\d\-: ]+)', '([\d\-: ]+)'\]"
                pattern_single = r"'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})'"

                postgis_sql = re.sub(pattern, convert_to_tsrange, postgis_sql)
                postgis_sql = postgis_sql.replace("OVERLAPS tsrange", "OVERLAPS")
                # postgis_sql = re.sub(pattern_single, convert_to_timestamptz, postgis_sql)
                
                postgis_queries.append(QueryTask(
                    name=query_config['name'],
                    type=query_config['type'],
                    sql=LiteralString(strip_comment_lines(postgis_sql)),
                    params=values
                ))

                pattern = r"\['([\d\-: ]+)', '([\d\-: ]+)'\]"
                pattern_single = r"'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})'"
                spacetime_queries.append(QueryTask(
                    name=query_config['name'],
                    type=query_config['type'],
                    sql=LiteralString(strip_comment_lines(spacetime_sql)),
                    params=values
                ))

                sedona_sql = fix_between_clause(sedona_sql)
                pattern = r"\['([\d\-: ]+)', '([\d\-: ]+)'\]"
                pattern_single = r"'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})'"
                sedona_queries.append(QueryTask(
                    name=query_config['name'],
                    type=query_config['type'],
                    sql=LiteralString(strip_comment_lines(sedona_sql)),
                    params=values
                ))

                # ClickHouse and DuckDB both use Postgres-compatible BETWEEN/ST_* syntax, same
                # as the postgis translation above -- give them the same post-processing.
                pattern = r"\['([\d\-: ]+)', '([\d\-: ]+)'\]"
                clickhouse_sql = re.sub(pattern, convert_to_tsrange, clickhouse_sql)
                clickhouse_sql = clickhouse_sql.replace("OVERLAPS tsrange", "OVERLAPS")
                clickhouse_queries.append(QueryTask(
                    name=query_config['name'],
                    type=query_config['type'],
                    sql=LiteralString(strip_comment_lines(clickhouse_sql)),
                    params=values
                ))

                duckdb_sql = re.sub(pattern, convert_to_tsrange, duckdb_sql)
                duckdb_sql = duckdb_sql.replace("OVERLAPS tsrange", "OVERLAPS")
                duckdb_queries.append(QueryTask(
                    name=query_config['name'],
                    type=query_config['type'],
                    sql=LiteralString(strip_comment_lines(duckdb_sql)),
                    params=values
                ))

    if config['benchmark']['mixed']:
        random.shuffle(sedona_queries)

    return mobilitydb_queries, postgis_queries, sedona_queries, spacetime_queries, clickhouse_queries, duckdb_queries
#main function
if __name__ == "__main__":
    config = load_config("../config/combinedBenchConf.yaml")
    if not config:
        print("Failed to load configuration. Please check the config file path and format.")    
        exit(1)
    print("Loaded config successfully.")
    thread_count = config['benchmark']['threads']
    nodes = config['benchmark']['nodes']
    sut = config['benchmark']['sut']
    main_seed = config['benchmark']['random_seed']
    mixed = config['benchmark']['mixed']
    distributed = len(nodes) > 1
    log_responses = config['benchmark']['test']
    #print these values
    print(f"Using SUT: {sut}")
    print(f"Using nodes: {nodes}")
    print(f"Using log responses: {log_responses}")
    print(f"Using main seed: {main_seed}")
    print(f"Using thread count: {thread_count}")
    print(f"Using mixed: {mixed}")
    print(f"Using distributed: {distributed}")

    main_random = random.Random(main_seed)
    #warm_up_random = random.Random(12345)

    print(f"Using random seed: {main_seed}")
    print(f"Using {thread_count} threads.")
    print(f"Mixed queries: {mixed}")
    print(f"Distributed: {distributed}")


    mobilityDB_queries,postgisSQL_queries, sedonaSQL_queries, spaceTimeSQL_queries, clickhouseSQL_queries, duckdbSQL_queries = prepare_query_tasks(config, main_random)

    num_queries = len(mobilityDB_queries)
    indices = list(range(num_queries))
    main_random.shuffle(indices)  # use the same seeded RNG

    # Apply the same permutation to all databases
    mobilityDB_queries   = [mobilityDB_queries[i] for i in indices]
    postgisSQL_queries   = [postgisSQL_queries[i] for i in indices]
    sedonaSQL_queries    = [sedonaSQL_queries[i] for i in indices]
    spaceTimeSQL_queries = [spaceTimeSQL_queries[i] for i in indices]
    clickhouseSQL_queries = [clickhouseSQL_queries[i] for i in indices]
    duckdbSQL_queries    = [duckdbSQL_queries[i] for i in indices]

    mobilityDB_data = [query.__dict__ for query in mobilityDB_queries]
    with open("../queries/mobilitydb_queries.yaml", "w") as file:
        yaml.dump(mobilityDB_data, file, sort_keys=False, allow_unicode=True)

    # (2026-10-02) the *_space/_time_partitioned MobilityDB variants are deprecated (moved to
    # resources/<scenario>/deprecated/, since 2026-10-10 kept locally in
    # single-node/paper/deprecated-queries/, outside git) and no longer generated.

    postgisSQL_data = [query.__dict__ for query in postgisSQL_queries]
    with open("../queries/postgisSQL_queries.yaml", "w") as file:
        yaml.dump(postgisSQL_data, file, sort_keys=False, allow_unicode=True)

    #create tsdb queries from postgisSQL_queries
    with open("../queries/postgisSQL_queries.yaml", "r") as file:
        content = file.read()
        content = content.replace('flight_points', 'tsdb_flight_points')
        content = content.replace('flight_trajectories', 'tsdb_flight_trajectories')
        with open("../queries/tsdb_queries.yaml", "w") as tsdb_file:
            tsdb_file.write(content)
    
    sedonaSQL_data = [query.__dict__ for query in sedonaSQL_queries]
    with open("../queries/sedonaSQL_queries.yaml", "w") as file:
        yaml.dump(sedonaSQL_data, file, sort_keys=False, allow_unicode=True)
    #Convert TIMESTAMP('2023-06-04 21:50:50') to TIMESTAMP '2023-06-04 21:50:50'
    with open("../queries/sedonaSQL_queries.yaml", "r") as file:
        content = file.read()
        content = re.sub(
            r"TIMESTAMP\s*\(\s*'([^']+)'\s*\)",
            r"TIMESTAMP '\1'",
            content
        )
        content = re.sub(
            r"\(ft\.start_time,\s*fe\.end_time\)\s*OVERLAPS\s*\[\s*'([^']+)'\s*,\s*'([^']+)'\s*\]",
            r"(ft.start_time <= TIMESTAMP '\2' AND fe.end_time >= TIMESTAMP '\1')",
            content
        )
        content = content.replace("DATE(st.start_ts)", "CAST(st.start_ts AS DATE)")
        with open("../queries/sedonaSQL_queries.yaml", "w") as sedona_file:
            sedona_file.write(content)

    spaceTimeSQL_data = [query.__dict__ for query in spaceTimeSQL_queries]
    with open("../queries/spaceTimeSQL_queries.yaml", "w") as file: 
        yaml.dump(spaceTimeSQL_data, file, sort_keys=False, allow_unicode=True)
    #adjust the file, i.e change timestamps from ['2023-10-11 05:15:27', '2023-10-19 01:32:06'] to '[2023-10-11 05:15:27, 2023-10-19 01:32:06]'
    
    with open("../queries/spaceTimeSQL_queries.yaml", "r") as file:
        content = file.read()
        content = adjust_timestamp_format(content)
        content = content.replace('SRID=3857;', '')
        # FIX: a blanket .replace("''", "'") here also collapsed YAML's own escaping of a
        # literal quote inside a single-quoted params scalar (e.g. the period_medium echo
        # ''2024-10-09 10:38:13'' AND ''2024-10-20 00:14:00'''), producing invalid YAML.
        # Restricted to non-params lines, where the doubled quote is a genuine SQL artifact.
        content = '\n'.join(
            l if re.match(r'^\s*-\s', l) else l.replace("''", "'")
            for l in content.split('\n')
        )
        #additonally, ST_ToGeom(ST_SetSRID(ST_Point(8.678265567853977, 52.010740125830374), 3857)::gpoint)) to ST_ToGeom(ST_Point(8.678265567853977, 52.010740125830374)::gpoint)

        pattern = re.compile(
            r"""ST_ToGeom\(\s*                 # ST_ToGeom(
                ST_SetSRID\(\s*               #   ST_SetSRID(
                ST_Point\(\s*([^)]+?)\s*\)\s* #     ST_Point(x, y)
                ,\s*3857\s*\)\s*              #   , 3857)
                ::\s*gpoint\s*\)           # ::gpoint)  optionally one extra ) at end
            """,
            re.X | re.IGNORECASE
        )

        replacement = r"ST_ToGeom(ST_Point(\1)::gpoint)"
        content = pattern.sub(replacement, content)
       
        with open("../queries/spaceTimeSQL_queries.yaml", "w") as file:
            file.write(content)
    convert_st_points_in_yaml("../queries/spaceTimeSQL_queries.yaml", "../queries/spaceTimeSQL_queries.yaml")
    with open("../queries/spaceTimeSQL_queries.yaml", "r") as file:
        content = file.read()
        #
        content = content.replace(", 3857)", "").replace("ST_SetSRID(", "").replace("ST_ToGeom(ST_Point(", "ST_ToGeom(ST_GeogPoint(")


        #remove all brackets [] from the file
    # with open("../queries/queries.yaml", "r") as file:
    #     content = file.read()
    #     content = content.replace('[', '').replace(']', '')
    # with open("../queries/queries.yaml", "w") as file:
    #     file.write(content)
    # print("Queries written to ../queries/queries.yaml")
    # #return all_queries
    clickhouseSQL_data = [query.__dict__ for query in clickhouseSQL_queries]
    with open("../queries/clickhouse_queries.yaml", "w") as file:
        yaml.dump(clickhouseSQL_data, file, sort_keys=False, allow_unicode=True)

    duckdbSQL_data = [query.__dict__ for query in duckdbSQL_queries]
    with open("../queries/duckdb_queries.yaml", "w") as file:
        yaml.dump(duckdbSQL_data, file, sort_keys=False, allow_unicode=True)

    #rep
    #copy all files to benchmark/client/benchmark_client/src/main/resources/aviation
    resource_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "..", "client", "benchmark_client", "src", "main", "resources", "aviation")
    shutil.copy("../queries/mobilitydb_queries.yaml", os.path.join(resource_path, "mobilitydb_queries.yaml"))
    shutil.copy("../queries/postgisSQL_queries.yaml", os.path.join(resource_path, "postgisSQL_queries.yaml"))
    shutil.copy("../queries/tsdb_queries.yaml", os.path.join(resource_path, "tsdb_queries.yaml"))
    sedona_crs.fix_file("../queries/sedonaSQL_queries.yaml")  # explicit CRS strings for ST_Transform (scripts/sedona_crs.py)
    shutil.copy("../queries/sedonaSQL_queries.yaml", os.path.join(resource_path, "sedonaSQL_queries.yaml"))
    shutil.copy("../queries/spaceTimeSQL_queries.yaml", os.path.join(resource_path, "spaceTimeSQL_queries.yaml"))
    shutil.copy("../queries/clickhouse_queries.yaml", os.path.join(resource_path, "clickhouse_queries.yaml"))
    shutil.copy("../queries/duckdb_queries.yaml", os.path.join(resource_path, "duckdb_queries.yaml"))