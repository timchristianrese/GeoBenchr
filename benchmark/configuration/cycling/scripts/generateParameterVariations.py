import random
from datetime import datetime, timedelta
from typing import List, Any, Dict, Optional
import csv
import os
import yaml
from dataclasses import dataclass
import re
import shutil
import psycopg2
import sys as _sys
_sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "scripts"))
import spacetime_units  # noqa: E402  (shared EPSG:3857 -> SpaceTime radius conversion)
import data_extent  # noqa: E402  (draws parameters where the data is)
data_extent.use("cycling")
import sedona_crs  # noqa: E402
# Example usage:
# convert_st_points_in_yaml("queries.yaml"

class LiteralString(str): pass

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

    Each live entry is an (id, timestamp) pair, not just an id: ride_points has no per-row primary
    key, so "id" alone identifies a ride (every point of it), not a single point. The *_single
    update/delete queries need the exact timestamp their scratch row was inserted with too, so
    pick_existing_pair/pop_existing_pair hand back both -- see updateridePointSingle/
    deleteridePointSingle in combinedBenchConf.yaml.
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

def generate_random_rider_id(rnd):
    return rnd.randint(1, 100000)

def generate_bulk_values(rnd, bbox, lower, upper, batch_size=10):
    # Returns (postgis_rows, clickhouse_rows): the same id/rider_id/timestamp/location per row,
    # formatted for each dialect's real columns -- ride_points has "geom" in postgis/mobilitydb/
    # duckdb but only plain "lat"/"lon" columns in ClickHouse (see insertSingleridePoint's FIX
    # note in combinedBenchConf.yaml).
    # FIX: rows didn't include "rider_id" -- ride_points has a rider_id column (see
    # insertSingleridePoint's FIX note) that every row needs.
    postgis_rows = []
    clickhouse_rows = []
    for _ in range(batch_size):
        lon, lat = get_random_lonlat(rnd, bbox)
        ts = generate_random_timestamp_in_bounds(rnd, lower, upper)
        rider_id = generate_random_rider_id(rnd)
        id_val = _SCRATCH_IDS.mint(ts)
        postgis_rows.append(f"({id_val}, {rider_id}, ST_Transform(ST_SetSRID(ST_Point({lon}, {lat}), 4326), 3857), TIMESTAMP '{ts}')")
        clickhouse_rows.append(f"({id_val}, {rider_id}, TIMESTAMP '{ts}', {lat}, {lon})")
    return ", ".join(postgis_rows), ", ".join(clickhouse_rows)



def literal_str_representer(dumper, data):
    return dumper.represent_scalar('tag:yaml.org,2002:str', data, style='|')

yaml.add_representer(LiteralString, literal_str_representer)

@dataclass
class QueryTask:
    name: str
    type: str
    sql: str
    params: list

def load_config(path="../config/combinedBenchConf.yaml"):
    try:
        with open(path) as f:
            return yaml.safe_load(f)
    except Exception as e:
        print(f"Failed to load config: {e}")
        return None

def convert_between_format(match):
    """
    Regex replacement function to convert BETWEEN format.
    """
    timestamp1 = match.group(1).strip()
    timestamp2 = match.group(2).strip()
    
    try:
        datetime.strptime(timestamp1, '%Y-%m-%d %H:%M:%S')
        datetime.strptime(timestamp2, '%Y-%m-%d %H:%M:%S')
    except ValueError:
        return match.group(0)
    
    return f"BETWEEN '{timestamp1}' AND '{timestamp2}'"

def adjust_timestamp_format_sedona(ts: str) -> str:
    pattern = r"TIMESTAMP\(\s*'\s*([\d\-: ]+)\s*'\s*\)"
    replacement = r"TIMESTAMP '\1'"
    return re.sub(pattern, replacement, ts)
def process_file(input_file, output_file):
    """
    Process a file to convert all BETWEEN clauses to proper SQL format.
    
    Args:
        input_file: Path to input file
        output_file: Path to output file
    """
    pattern = re.compile(
        r"BETWEEN\s*\[\s*'([^']+)'\s*,\s*'([^']+)'\s*\]",
        re.IGNORECASE
    )
    
    with open(input_file, 'r') as infile, open(output_file, 'w') as outfile:
        for line in infile:
            converted_line = pattern.sub(convert_between_format, line)
            outfile.write(converted_line)

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

def return_param_values(bbox, lower, upper, sql1, sql2, sql3, sql4, sql5, sql6, params, rnd, query_type) -> List[Any]:
    parsedSQL1, parsedSQL2, parsedSQL3, parsedSQL4 = sql1, sql2, sql3, sql4
    parsedSQL5, parsedSQL6 = sql5, sql6  # clickhouse, duckdb
    values = []
    formatter = "%Y-%m-%d %H:%M:%S"

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
            "day": lambda: get_random_day(rnd, lower, upper),
            "university": lambda: get_random_place("universities", rnd),
            "district": lambda: get_random_place("berlin-districts", rnd),
            "point": lambda: get_random_point(rnd, [[6.212909, 52.241256], [8.752841, 50.53438]]),
            "radius": lambda: rnd.randint(100,1000), #in meters
            "distance": lambda: rnd.randint(500,2000),  # in meters (2026-10-18, was 100-1000)
            "rider_id": lambda: generate_random_rider_id(rnd),
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
                parsedSQL5 = substitute_outside_comments(parsedSQL5, ":bulk_values_clickhouse", replacement[1])
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

            # ClickHouse has no PostGIS-style point/geometry type -- if a query ever decomposes
            # ":point" into ":point_lon"/":point_lat" for that dialect only, resolve them here,
            # before the generic ":point" replace below clobbers the ":point" prefix inside them.
            if param == "point" and isinstance(replacement, str):
                point_coords = re.search(r"ST_Point\(\s*([^,]+?)\s*,\s*([^)]+?)\s*\)", replacement)
                if point_coords:
                    parsedSQL5 = substitute_outside_comments(parsedSQL5, ":point_lon", point_coords.group(1))
                    parsedSQL5 = substitute_outside_comments(parsedSQL5, ":point_lat", point_coords.group(2))

            # FIX: ClickHouse (and, less certainly, DuckDB) have no OVERLAPS operator -- queries
            # that need "(start, end) OVERLAPS :period_x" have to be rewritten as an explicit
            # "start_time < :period_x_end AND :period_x_start < end_time" boolean instead, which
            # needs the period's two bounds as separate scalars. :period_x itself only ever
            # arrives as one opaque packed "'ts1' AND 'ts2'" string (see
            # generate_random_time_span_in_bounds above), so combinedBenchConf.yaml's
            # clickhouse/duckdb blocks couldn't actually use :period_x_start/:period_x_end
            # despite ais's copy of this script's equivalent query already anticipating that
            # exact naming. Decomposing here, before the generic :{param} substitution below,
            # the same way :point is split into :point_lon/:point_lat above.
            if param.startswith("period") and isinstance(replacement, str):
                span_match = re.match(r"'([^']+)' AND '([^']+)'", replacement)
                if span_match:
                    ts1, ts2 = span_match.group(1), span_match.group(2)
                    parsedSQL5 = substitute_outside_comments(parsedSQL5, f":{param}_start", f"'{ts1}'")
                    parsedSQL5 = substitute_outside_comments(parsedSQL5, f":{param}_end", f"'{ts2}'")
                    parsedSQL6 = substitute_outside_comments(parsedSQL6, f":{param}_start", f"'{ts1}'")
                    parsedSQL6 = substitute_outside_comments(parsedSQL6, f":{param}_end", f"'{ts2}'")

            # FIX: replacement is now a (postgis_rows, clickhouse_rows) tuple for "bulk_values" --
            # PyYAML would serialize a bare tuple with its own "!!python/tuple" tag, which Java's
            # SnakeYAML (the client's actual YAML reader) doesn't recognize (confirmed live for the
            # equivalent aviation/scripts case). params: is only ever informational documentation
            # of what was substituted (not read back by the client), so the postgis-shaped half is
            # fine here.
            values.append(quoted if param == "bulk_values" else replacement)
            # FIX: same as aviation/ais's generateParameterVariations.py -- generate_random_time_
            # span_in_bounds() returns "'ts1' AND 'ts2'" for postgis/sedona/clickhouse/duckdb's
            # BETWEEN usage, but mobilitydb's templates use the same :period_x as an operand to
            # && or inside attime(), needing a real tstzspan literal instead. Give mobilitydb its
            # own substitution here, before the shared one below runs unchanged for the other
            # five dialects.
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

    return parsedSQL1, parsedSQL2, parsedSQL3, parsedSQL4, parsedSQL5, parsedSQL6, values


# -----------------
# Helpers
# -----------------

def get_spatial_bounds(table, geom_col="geom"):
    #check if file exists with the spatial bounds, if yes, read from file and return
    if os.path.exists("spatial_bounds.txt"):
        with open("spatial_bounds.txt", "r") as f:
            print("Reading spatial bounds from file...")
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
        xmin, ymin, xmax, ymax = cur.fetchone()
    # same format as before: [[ul_lon, ul_lat], [br_lon, br_lat]]
    # save the spatial bounds to a file and check the file to prevent executing the request in the future
    with open("spatial_bounds.txt", "w") as f:
        f.write(f"{xmin},{ymin},{xmax},{ymax}")
    return [[xmin, ymax], [xmax, ymin]]

def get_random_lonlat(rnd, rect):
    ll = data_extent.lonlat(rnd)
    if ll:
        return ll
    ul_lon, ul_lat = rect[0]
    br_lon, br_lat = rect[1]

    lon = ul_lon + rnd.random() * (br_lon - ul_lon)
    lat = br_lat + rnd.random() * (ul_lat - br_lat)
    return lon, lat

def get_random_point(rnd, rect):
    lon, lat = get_random_lonlat(rnd, rect)
    return f"ST_Transform(ST_SetSRID(ST_Point({lon}, {lat}), 4326), 3857)"



def get_time_bounds(table, time_col="timestamp_col"):
    print("Checking for time bounds in file...")
    if os.path.exists("time_bounds.txt"):
        with open("time_bounds.txt", "r") as f:
            bounds = f.read().strip().split(",")
            lower = datetime.strptime(bounds[0], "%Y-%m-%d %H:%M:%S")
            upper = datetime.strptime(bounds[1], "%Y-%m-%d %H:%M:%S")
            return lower, upper
    print("Time bounds not found in file. Querying database...")
    # with conn.cursor() as cur:
    #     cur.execute(f"SELECT MIN({time_col}), MAX({time_col}) FROM {table};")
    #     lower, upper = cur.fetchone()
    with open("time_bounds.txt", "w") as f:
        f.write(f"{lower},{upper}")
    return lower, upper

def get_random_day(rnd, lower,upper):
    #get random day between lower and upper timestamp
    delta = (upper - lower).days
    day_offset = rnd.randint(0, delta)
    random_day = lower + timedelta(days=day_offset)
    return random_day.strftime("%Y-%m-%d")

def generate_random_timestamp_in_bounds(rnd, lower, upper):
    delta = (upper - lower).total_seconds()
    ts = data_extent.instant(rnd) or lower + timedelta(seconds=rnd.uniform(0, delta))
    return ts.strftime("%Y-%m-%d %H:%M:%S")

def generate_random_time_span_in_bounds(rnd, lower, upper, mode=0):
    delta = (upper - lower).total_seconds()
    mode_ranges = {
        1: (60 * 60, 4 * 60 * 60),          # short: 1-4 h (2026-10-18, was 0.5-2 h)
        2: (2*24*60*60, 15*24*60*60), # medium
        3: (15*24*60*60, delta)       # long
    }
    seconds = min(rnd.uniform(*mode_ranges.get(mode, (0, delta))), delta)
    # (2026-10-18) the window keeps its full length: it starts early enough to end within the data
    # (before, a window running past either end was cut at it, down to a few minutes)
    start = data_extent.window_start(rnd, lower, upper, seconds) or lower + timedelta(seconds=rnd.uniform(0, delta - seconds))
    end = start + timedelta(seconds=seconds)
    return f"'{start.strftime('%Y-%m-%d %H:%M:%S')}' AND '{end.strftime('%Y-%m-%d %H:%M:%S')}'"


def get_random_place(filename: str, rnd: random.Random) -> Optional[str]:
    pre = data_extent.place(filename)  # the place drawn together with this query's time parameters
    if pre is not None:
        return pre
    filepath = os.path.join("..", "data", f"{filename}.csv")
    try:
        with open(filepath, newline='', encoding='utf-8') as csvfile:
            reader = list(csv.reader(csvfile))
            if len(reader) <= 1: return None
            rows = reader[1:]
            ok = data_extent.allowed(filename)
            if ok is not None:
                col = 4 if "cities" in filepath else 0
                rows = [r for r in rows if len(r) > col and r[col] in ok] or rows
            chosen_row = rnd.choice(rows)
            return chosen_row[0]
    except Exception as e:
        print(f"Error reading {filepath}: {e}")
        return None


# -----------------
# Regex helpers
# -----------------

def convert_to_tstzspan(match): return f"tstzspan '[{match.group(1)}, {match.group(2)}]'"

def convert_to_tsrange(match): return f"tsrange '[{match.group(1)}, {match.group(2)}]'"

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

def fix_between_clause(sql_fragment: str) -> str:
    pattern = r"BETWEEN\s*\[\s*'([^']+)'\s*,\s*'([^']+)'\s*\]"
    replacement = r"BETWEEN TIMESTAMP('\1') AND TIMESTAMP('\2')"
    return re.sub(pattern, replacement, sql_fragment)

def adjust_timestamp_format(ts: str) -> str:
    pattern = r"\['([\d\-: ]+)', '([\d\-: ]+)'\]"
    replacement = r"'[\1, \2]'"
    return re.sub(pattern, replacement, ts)


# -----------------
# Main preparation
# -----------------

def prepare_query_tasks(config, rnd) -> List[QueryTask]:
    mobilitydb_queries, postgis_queries, sedona_queries, spacetime_queries = [], [], [], []
    clickhouse_queries, duckdb_queries = [], []
    bbox = get_spatial_bounds("ride_points", "geom")

    lower, upper = get_time_bounds("ride_points", "timestamp")
    print(bbox, lower, upper)

    for query_config in config['queryConfigs']:
        if query_config['use']:
            for _ in range(query_config['repetition']):
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
                spacetime_sql = spacetime_units.apply(spacetime_sql, dict(zip(query_config['parameters'], values)), 52.5)

                pattern = r"\['([\d\-: ]+)', '([\d\-: ]+)'\]"
                # See convert_to_timestamptz()'s FIX comment above -- it now handles the
                # already-cast (bulk_insert) and column-equality cases itself, so this stays a
                # plain pattern (same as every other scenario's generator script).
                pattern_single = r"'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})'"

                mobilitydb_sql = re.sub(pattern, convert_to_tstzspan, mobilitydb_sql)
                mobilitydb_sql = re.sub(pattern_single, convert_to_timestamptz, mobilitydb_sql)
                mobilitydb_queries.append(QueryTask(query_config['name'], query_config['type'], LiteralString(strip_comment_lines(mobilitydb_sql)), values))

                postgis_sql = re.sub(pattern,convert_to_tsrange, postgis_sql)
                #postgis_sql = re.sub(pattern_single, convert_to_timestamptz, postgis_sql)
                postgis_queries.append(QueryTask(query_config['name'], query_config['type'], LiteralString(strip_comment_lines(postgis_sql)), values))
                sedona_queries.append(QueryTask(query_config['name'], query_config['type'], LiteralString(strip_comment_lines(fix_between_clause(sedona_sql))), values))
                spacetime_queries.append(QueryTask(query_config['name'], query_config['type'], LiteralString(strip_comment_lines(spacetime_sql)), values))

                # ClickHouse and DuckDB: same Postgres-style BETWEEN/list-bracket substitution
                # shape as postgis above -- resolved by the same unprocess -> process_file step.
                clickhouse_sql = re.sub(pattern, convert_to_tsrange, clickhouse_sql)
                clickhouse_queries.append(QueryTask(query_config['name'], query_config['type'], LiteralString(strip_comment_lines(clickhouse_sql)), values))
                duckdb_sql = re.sub(pattern, convert_to_tsrange, duckdb_sql)
                duckdb_queries.append(QueryTask(query_config['name'], query_config['type'], LiteralString(strip_comment_lines(duckdb_sql)), values))

    if config['benchmark']['mixed']:
        random.shuffle(sedona_queries)

    return mobilitydb_queries, postgis_queries, sedona_queries, spacetime_queries, clickhouse_queries, duckdb_queries


# -----------------
# Main
# -----------------

if __name__ == "__main__":
    config = load_config("../config/combinedBenchConf.yaml")
    if not config:
        print("Failed to load configuration.")
        exit(1)

    print("Loaded config successfully.")
    thread_count = config['benchmark']['threads']
    nodes = config['benchmark']['nodes']
    sut = config['benchmark']['sut']
    main_seed = config['benchmark']['random_seed']
    mixed = config['benchmark']['mixed']
    distributed = len(nodes) > 1

    main_random = random.Random(main_seed)

    mobilityDB_queries, postgisSQL_queries, sedonaSQL_queries, spaceTimeSQL_queries, clickhouseSQL_queries, duckdbSQL_queries = prepare_query_tasks(config, main_random)

    num_queries = len(mobilityDB_queries)
    indices = list(range(num_queries))
    main_random.shuffle(indices)

    mobilityDB_queries   = [mobilityDB_queries[i] for i in indices]
    postgisSQL_queries   = [postgisSQL_queries[i] for i in indices]
    sedonaSQL_queries    = [sedonaSQL_queries[i] for i in indices]
    spaceTimeSQL_queries = [spaceTimeSQL_queries[i] for i in indices]
    clickhouseSQL_queries = [clickhouseSQL_queries[i] for i in indices]
    duckdbSQL_queries    = [duckdbSQL_queries[i] for i in indices]

    # MobilityDB
    with open("../queries/mobilitydb_queries.yaml", "w") as f:
        yaml.dump([q.__dict__ for q in mobilityDB_queries], f, sort_keys=False, allow_unicode=True)

    # (2026-10-02) the *_space/_time_partitioned MobilityDB variants are deprecated (moved to
    # resources/<scenario>/deprecated/, since 2026-10-10 kept locally in
    # single-node/paper/deprecated-queries/, outside git) and no longer generated.
    # PostGIS
    with open("../queries/postgisSQL_queries_unprocess.yaml", "w") as f:
        yaml.dump([q.__dict__ for q in postgisSQL_queries], f, sort_keys=False, allow_unicode=True)
    process_file("../queries/postgisSQL_queries_unprocess.yaml", "../queries/postgisSQL_queries.yaml")

    # TSDB variant
    with open("../queries/postgisSQL_queries.yaml", "r") as f:
        content = f.read().replace("ride_points", "tsdb_ride_points")
        content = content.replace("ride_trajectories", "tsdb_ride_trajectories")
        with open("../queries/tsdb_queries.yaml", "w") as tsdb_file:
            tsdb_file.write(content)

    # Sedona
    with open("../queries/sedonaSQL_queries.yaml", "w") as f:
        yaml.dump([q.__dict__ for q in sedonaSQL_queries], f, sort_keys=False, allow_unicode=True)
    #adjust timestamp, i.e TIMESTAMP('2023-06-04 21:50:50') to literal TIMESTAMP '2023-06-04 21:50:50'
    with open("../queries/sedonaSQL_queries.yaml", "r") as f:
        content = adjust_timestamp_format_sedona(f.read())
        pattern = r"""
        \(rs\.start_time,\s*re\.end_time\)      # the columns
        \s*OVERLAPS\s*                          # the OVERLAPS keyword
        \(\s*'([^']+)'\s*::timestamp\s*,\s*     # first timestamp
        '([^']+)'\s*::timestamp(?:\s*\+\s*INTERVAL\s*'([^']+)')?\s*\)  # second timestamp + optional interval
        """

        def overlaps_replacement(match):
            start_ts = match.group(1)
            end_base = match.group(2)
            interval = match.group(3)
            if interval:
                return f"(rs.start_time <= TIMESTAMP '{end_base}' + INTERVAL '{interval}' AND re.end_time >= TIMESTAMP '{start_ts}')"
            else:
                return f"(rs.start_time <= TIMESTAMP '{end_base}' AND re.end_time >= TIMESTAMP '{start_ts}')"

        # Perform replacement
        content = re.sub(pattern, overlaps_replacement, content, flags=re.VERBOSE)
    with open("../queries/sedonaSQL_queries.yaml", "w") as f:
        f.write(content)
    # Spacetime
    with open("../queries/spaceTimeSQL_queries.yaml", "w") as f:
        yaml.dump([q.__dict__ for q in spaceTimeSQL_queries], f, sort_keys=False, allow_unicode=True)
    with open("../queries/spaceTimeSQL_queries.yaml", "r") as f:
        content = adjust_timestamp_format(f.read())
    
    with open("../queries/spaceTimeSQL_queries.yaml", "w") as f:
        f.write(content)

    # ClickHouse and DuckDB
    with open("../queries/clickhouse_queries_unprocess.yaml", "w") as f:
        yaml.dump([q.__dict__ for q in clickhouseSQL_queries], f, sort_keys=False, allow_unicode=True)
    process_file("../queries/clickhouse_queries_unprocess.yaml", "../queries/clickhouse_queries.yaml")

    with open("../queries/duckdb_queries_unprocess.yaml", "w") as f:
        yaml.dump([q.__dict__ for q in duckdbSQL_queries], f, sort_keys=False, allow_unicode=True)
    process_file("../queries/duckdb_queries_unprocess.yaml", "../queries/duckdb_queries.yaml")

    #copy all created files to benchmark/client/benchmark_client/src/main/resources/cycling

    resource_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "..", "client", "benchmark_client", "src", "main", "resources", "cycling")
    shutil.copy("../queries/mobilitydb_queries.yaml", os.path.join(resource_path, "mobilitydb_queries.yaml"))
    shutil.copy("../queries/postgisSQL_queries.yaml", os.path.join(resource_path, "postgisSQL_queries.yaml"))
    shutil.copy("../queries/tsdb_queries.yaml", os.path.join(resource_path, "tsdb_queries.yaml"))
    sedona_crs.fix_file("../queries/sedonaSQL_queries.yaml")  # explicit CRS strings for ST_Transform (scripts/sedona_crs.py)
    shutil.copy("../queries/sedonaSQL_queries.yaml", os.path.join(resource_path, "sedonaSQL_queries.yaml"))
    shutil.copy("../queries/spaceTimeSQL_queries.yaml", os.path.join(resource_path, "spaceTimeSQL_queries.yaml"))
    shutil.copy("../queries/clickhouse_queries.yaml", os.path.join(resource_path, "clickhouse_queries.yaml"))
    shutil.copy("../queries/duckdb_queries.yaml", os.path.join(resource_path, "duckdb_queries.yaml"))