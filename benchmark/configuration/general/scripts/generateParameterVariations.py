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
import math 
import sys as _sys
_sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "scripts"))
import spacetime_units  # noqa: E402  (shared EPSG:3857 -> SpaceTime radius conversion)

DATASET_CONFIG = {
    "ais": {
        "table": "crossing_points",
        "spatial_bounds_file": "spatial_bounds_ais.txt",
        "time_bounds_file": "time_bounds_ais.txt",
        # FIX (2026-09-30): added for the general/ scenario's MobilityDB queries -- see
        # "trajectory_table"'s use in prepare_query_tasks below. crossings/flights/rides are the
        # MobilityDB-native tgeompoint trajectory tables (one row per trip), distinct from the
        # flat "table" above (one row per point) that every other dialect here uses via
        # :dataset. geolife has no trajectory-table equivalent (see below) -- queries that need
        # one are skipped for mobilitydb specifically when running against geolife.
        "trajectory_table": "crossings",
        # FIX (2026-10-01): crossings/flights store the naive wall-clock digits as UTC instants;
        # rides holds real instants (ride_points is timestamptz). See :trajectory_tz.
        "trajectory_tz": "UTC",
        # SpaceTime: id column and trip-level table, as in Ceperic's general.maritime.yaml
        "spacetime_id": "crossing_id",
        "spacetime_trip_table": "crossings",
        # (2026-10-02) queries with a "<dialect>_trip_summary" variant (timeInterval) use it here,
        # against this table, to ask SpaceTime's trip-span question (its "crossings" is a summary)
        "trip_summary": "crossings_summary",
        # (2026-10-02) same id filter as every ais scenario query: excludes the id=0 sentinel
        # (SpaceTime, Sedona) and the remapped >= 1e12 singleton points (Postgres, ClickHouse, DuckDB)
        "id_filter": True,
    },
    "aviation": {
        "table": "flight_points",
        "spatial_bounds_file": "spatial_bounds_aviation.txt",
        "time_bounds_file": "time_bounds_aviation.txt",
        "trajectory_table": "flights",
        "trajectory_tz": "UTC",
        "spacetime_id": "flight_id",
        "spacetime_trip_table": "flight_points",
    },
    "geolife": {
        "table": "geolife_points",
        "spatial_bounds_file": "spatial_bounds_geolife.txt",
        "time_bounds_file": "time_bounds_geolife.txt",
        "spacetime_id": "user_id",
        "spacetime_trip_table": "geolife_points",
        # No trajectory-table equivalent exists for geolife (confirmed: no
        # "geolife_trajectories"/"users" table with a temporal "trip" column anywhere in this
        # project) -- left absent on purpose, not an oversight. See prepare_query_tasks below.
    },
    "cycling": {
        "table": "ride_points",
        "spatial_bounds_file": "spatial_bounds_cycling.txt",
        "time_bounds_file": "time_bounds_cycling.txt",
        "trajectory_table": "rides",
        "trajectory_tz": "Europe/Berlin",
        "spacetime_id": "trip_id",
        "spacetime_trip_table": "ride_points",
    }
}


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

def get_scaled_radius(rnd, bbox):
    """
    Generate a radius in meters based on the dataset bounding box size.

    bbox:
        [[min_lon, max_lat], [max_lon, min_lat]]

    Returns:
        radius in meters
    """

    ul_lon, ul_lat = bbox[0]
    br_lon, br_lat = bbox[1]

    # Degree differences
    lon_diff = abs(br_lon - ul_lon)
    lat_diff = abs(ul_lat - br_lat)

    # Approximate degree -> meter conversion
    # 1 degree latitude ~= 111,320 meters
    lat_meters = lat_diff * 111320

    # Longitude distance depends on latitude
    mean_lat = (ul_lat + br_lat) / 2
    lon_meters = lon_diff * 111320 * math.cos(math.radians(mean_lat))

    # Diagonal length of bbox in meters
    diagonal_meters = math.sqrt(
        lat_meters ** 2 + lon_meters ** 2
    )

    # radius between 3-6% of bbox diagonal (2026-10-18, was 1-3 %)
    return rnd.uniform(0.03, 0.06) * diagonal_meters

def generate_fixed_time_span(rnd, lower, upper, fraction=0.01):
    total_seconds = (upper - lower).total_seconds()
    span = total_seconds * fraction

    start = lower + timedelta(seconds=rnd.uniform(0, total_seconds - span))
    end = start + timedelta(seconds=span)

    return f"'{start.strftime('%Y-%m-%d %H:%M:%S')}' AND '{end.strftime('%Y-%m-%d %H:%M:%S')}'"

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


def spacetime_literal(value):
    """FIX (2026-10-02): SpaceTime takes regions/points as WKT strings cast to gpolygon/gpoint, in
    lon/lat, and periods as EPOCHRANGE '[a, b]' (same conventions as Ceperic's own general.*.yaml
    parameter files on nst-inserter). The shared renderings are PostGIS-only
    (ST_Transform(ST_MakeEnvelope(...)), "'a' AND 'b'"), which SpaceTime rejected: "unknown
    function: st_makeenvelope" / parse_error on every general query."""
    if not isinstance(value, str):
        return value
    num = r"\s*(-?[\d.]+)\s*"
    env = re.search(rf"ST_MakeEnvelope\({num},{num},{num},{num}", value)
    if env:
        x1, y1, x2, y2 = env.groups()
        return f"'POLYGON (({x1} {y1}, {x2} {y1}, {x2} {y2}, {x1} {y2}, {x1} {y1}))'"
    pt = re.search(rf"ST_Point\({num},{num}\)", value)
    if pt:
        return f"'POINT ({pt.group(1)} {pt.group(2)})'"
    span = re.fullmatch(r"\s*'([\d\-: ]+)'\s+AND\s+'([\d\-: ]+)'\s*", value)
    if span:
        return f"'[{span.group(1)}, {span.group(2)}]'"
    return value

def return_param_values(bbox, lower,upper, sql1,sql2, sql3, sql4, sql5, sql6, params, rnd) -> List[Any]:
    parsedSQL1 = sql1
    parsedSQL2 = sql2
    parsedSQL3 = sql3
    print("SQL3 before processing:", parsedSQL3)
    parsedSQL4 = sql4
    parsedSQL5 = sql5  # clickhouse
    parsedSQL6 = sql6  # duckdb
    values = []
    formatter = "%Y-%m-%d %H:%M:%S"  # Default formatter for datetime


    for param in params:
        replacement = {
            "region": lambda: generate_random_region_bbox(rnd, bbox, coverage=0.03),  # 3 % of the area (2026-10-18, was 1 %)
            "region1": lambda: generate_random_region_bbox(rnd, bbox, coverage=0.03),  # 3 % of the area (2026-10-18, was 1 %)
            "second_region": lambda: generate_random_region_bbox(rnd, bbox, coverage=0.03),  # 3 % of the area (2026-10-18, was 1 %)
            "region_medium": lambda: generate_random_region_bbox(rnd, bbox, coverage=0.05),
            "region_medium2": lambda: generate_random_region_bbox(rnd, bbox, coverage=0.05),
            "radius": lambda: get_scaled_radius(rnd, bbox),
            "distance": lambda: get_scaled_radius(rnd, bbox), 
            "period": lambda: generate_fixed_time_span(rnd, lower, upper, 0.03),  # 3 % of the time span (2026-10-18, was 1 %)
            "point" : lambda: get_random_point(rnd, bbox),
            "k": lambda: rnd.randint(1, 10),
        }.get(param, lambda: "")()

        if replacement is not None:
            if isinstance(replacement, str):
                # Detect SQL expressions like ST_*, SELECT, or functions with parentheses
                # FIX: generate_fixed_time_span() (the "period" replacement) already returns a
                # fully-quoted "'start' AND 'end'" string, since :period expands into a BETWEEN
                # clause's two operands, not one scalar. Falling through to the generic
                # f"'{replacement}'" wrapping below double-quoted it into
                # "''start' AND 'end''" -- invalid SQL that was silently shipping in every
                # generated query using :period (confirmed in the committed
                # aviation/postgisSQL_queries.yaml, e.g. "BETWEEN ''2024-10-09 10:38:13' AND
                # '2024-10-20 00:14:00''"). param == "period" must be treated the same as the
                # SQL-expression case: leave it unquoted, since it's already quoted.
                if param == "period" or replacement.strip().upper().startswith(("ST_", "SELECT", "POINT(", "POLYGON(", "LINESTRING(")):
                    quoted = replacement  # leave unquoted (SQL expression)
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
            # (see the FIX comment on the kNN clickhouse: block in combinedBenchConf.yaml).
            # This must run before the generic ":point" replace below -- str.replace(":point", ...)
            # would otherwise also match the ":point" prefix inside ":point_lon"/":point_lat".
            if param == "point" and isinstance(replacement, str):
                point_coords = re.search(r"ST_Point\(\s*([^,]+?)\s*,\s*([^)]+?)\s*\)", replacement)
                if point_coords:
                    parsedSQL5 = substitute_outside_comments(parsedSQL5, ":point_lon", point_coords.group(1))
                    parsedSQL5 = substitute_outside_comments(parsedSQL5, ":point_lat", point_coords.group(2))

            # FIX (2026-09-30): same ClickHouse-has-no-PostGIS-geometry-type gap as :point above,
            # for region/bbox parameters -- SpatialBBox/spatioTemporalRange/
            # IDsInsideRegion1Andsecond_region's clickhouse blocks use pointInPolygon((lon, lat),
            # :region), but :region's actual value is a PostGIS-only
            # "ST_Transform(ST_SetSRID(ST_MakeEnvelope(x1,y1,x2,y2),4326),3857)" expression --
            # confirmed live, every one of these clickhouse queries failed outright with
            # "Function with name `ST_MakeEnvelope` does not exist". Decomposes into
            # ":region_ch"/":second_region_ch" -- a ClickHouse Ring literal (four corners, raw
            # WGS84 degrees, matching the unprojected (lon, lat) tuple these queries compare
            # against -- no Web Mercator reprojection needed here, unlike the point-to-point/
            # point-to-polygon distance fixes elsewhere, since pointInPolygon is a pure
            # containment test in whatever coordinate system both operands share). Written with
            # array()/tuple() function-call syntax rather than [[(...)]] literal syntax for the
            # same bracket-mangling-safety reason as ais's process_file() (see that file's
            # countActiveCrossingsNearIsland clickhouse block) -- this scenario's own
            # process_file() isn't confirmed safe either way, so playing it safe here too.
            if param in ("region", "region1", "second_region", "region_medium", "region_medium2") and isinstance(replacement, str):
                envelope_coords = re.search(
                    r"ST_MakeEnvelope\(\s*([^,]+?)\s*,\s*([^,]+?)\s*,\s*([^,]+?)\s*,\s*([^)]+?)\s*\)",
                    replacement,
                )
                if envelope_coords:
                    x1, y1, x2, y2 = envelope_coords.groups()
                    ring = f"array(tuple({x1}, {y1}), tuple({x2}, {y1}), tuple({x2}, {y2}), tuple({x1}, {y2}))"
                    parsedSQL5 = substitute_outside_comments(parsedSQL5, f":{param}_ch", ring)

            values.append(replacement)
            # FIX (2026-09-30): generate_fixed_time_span() (the ":period" replacement) returns a
            # "'start' AND 'end'" string meant to slot after postgis/sedona/clickhouse/duckdb's
            # "BETWEEN :period" -- correct for those five, but mobilitydb's new trajectory-based
            # queries (timeInterval/timeAggregation/spatioTemporalRange/
            # IDsInsideRegion1Andsecond_region, added the same day this was found) use :period as
            # an operand to && (trip && :period) or inside atTime(), which needs a real tstzspan
            # literal instead -- substituting the raw "'start' AND 'end'" text there produced
            # "trip && timestamptz '[start]' AND timestamptz '[end]'" (each half independently
            # timestamptz-wrapped by the pattern_single regex further down in prepare_query_tasks,
            # since neither half is bracketed the way that regex's sibling `pattern` expects) --
            # invalid SQL, confirmed live. This project's other scenario-specific generator
            # scripts (aviation/scripts, cycling/scripts) already have this same fix for their own
            # :period-using mobilitydb queries; general/scripts never needed it before since every
            # mobilitydb: block here was blank until now. Give mobilitydb its own substitution,
            # before the shared one below runs unchanged for the other five dialects.
            if param == "period" and isinstance(replacement, str):
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
            parsedSQL4 = substitute_outside_comments(parsedSQL4, f":{param}", spacetime_literal(quoted))
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
    
    # FIX (2026-09-30): was "min_lat, min_lon = bbox[0]" / "max_lat, max_lon = bbox[1]" -- same
    # bug class as get_random_point above: bbox[0]/bbox[1] are actually [lon, lat] pairs (per
    # get_spatial_bounds's own construction), so this silently swapped lon/lat into wrongly-named
    # variables, scrambling both axes (lon_span ended up computed from the two lat values with a
    # sign flip, etc). Confirmed live: aviation's bounds (lon 5.5-9.5, lat 50.3-52.6) produced
    # ST_MakeEnvelope(58.88, 7.32, 58.07, 9.06) -- 58.88/58.07 are valid coordinates for neither
    # axis of this bounding box.
    min_lon, max_lat = bbox[0]
    max_lon, min_lat = bbox[1]

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

def get_time_bounds(table, time_col="timestamp", file_name="time_bounds.txt"):
    if os.path.exists(file_name):
        with open(file_name, "r") as f:
            bounds = f.read().strip().split(",")
            lower = datetime.strptime(bounds[0], "%Y-%m-%d %H:%M:%S")
            upper = datetime.strptime(bounds[1], "%Y-%m-%d %H:%M:%S")
            return lower, upper



    with open(file_name, "w") as f:
        f.write(f"{lower},{upper}")

    return lower, upper


def generate_random_timestamp_in_bounds(rnd, lower, upper):
    delta = (upper - lower).total_seconds()
    ts = lower + timedelta(seconds=rnd.uniform(0, delta))
    return ts.strftime("%Y-%m-%d %H:%M:%S")

def generate_random_time_span_in_bounds(rnd, lower, upper, mode=0):
    delta = (upper - lower).total_seconds()
    mode_ranges = {
        1: (0, 2*24*60*60),         # short
        2: (2*24*60*60, 4*24*60*60), # medium
        3: (4*24*60*60, 8*24*60*60)   # long
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
def get_spatial_bounds(table, geom_col="geom", file_name="spatial_bounds.txt"):
    if os.path.exists(file_name):
        with open(file_name, "r") as f:
            bounds = f.read().strip().split(",")
            return [[float(bounds[0]), float(bounds[3])], [float(bounds[2]), float(bounds[1])]]


def get_random_point(rnd, rect):
    # FIX (2026-09-30): was "ul_lat, ul_lon = rect[0]" / "br_lat, br_lon = rect[1]" -- rect[0]/
    # rect[1] are actually [lon, lat] pairs (see get_spatial_bounds above: bounds[0]/bounds[2] are
    # the file's min_lon/max_lon columns, bounds[3]/bounds[1] its max_lat/min_lat columns), so this
    # silently swapped lon and lat into wrongly-named variables -- confirmed live: aviation's
    # bounds (lon 5.5-9.5, lat 50.3-52.6) produced ST_Point(50.76, 6.12), i.e. a point near the
    # equator off the African coast instead of the Netherlands/Germany. aviation's own copy of
    # this function (aviation/scripts/generateParameterVariations.py) already unpacks rect[0]/
    # rect[1] the correct way round -- matched that here.
    ul_lon, ul_lat = rect[0]
    br_lon, br_lat = rect[1]
    lon = ul_lon + rnd.random() * (br_lon - ul_lon)
    lat = br_lat + rnd.random() * (ul_lat - br_lat)
    return f"ST_Transform(ST_SetSRID(ST_Point({lon}, {lat}), 4326), 3857)"

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
    filepath = os.path.join("..", "data", f"{filename}.csv")
    
    try:
        with open(filepath, newline='', encoding='utf-8') as csvfile:
            reader = list(csv.reader(csvfile))
            if len(reader) <= 1:
                return None  # No data or only header
            chosen_row = rnd.choice(reader[1:])  # Skip header
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
    #     keywords back to back, a syntax error. Skipped when already cast.
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




def inject_dataset(sql, table_name):
    return substitute_outside_comments(sql, ":dataset", table_name)

def prepare_query_tasks(config, rnd) -> List[QueryTask]:
    mobilitydb_queries = []
    postgis_queries = []
    sedona_queries = []
    spacetime_queries = []
    clickhouse_queries = []
    duckdb_queries = []


    dataset_key = config['benchmark'].get("dataset", "geolife")
    dataset_conf = DATASET_CONFIG[dataset_key]

    table_name = dataset_conf["table"]
    # FIX (2026-09-30): see DATASET_CONFIG's comment above -- None for geolife (no trajectory
    # table exists), a real tgeompoint table name otherwise.
    trajectory_table = dataset_conf.get("trajectory_table")

    lower, upper = get_time_bounds(
        table_name,
        "timestamp",
        dataset_conf["time_bounds_file"]
    )

    bbox = get_spatial_bounds(
        table_name,
        "geom",
        dataset_conf["spatial_bounds_file"]
    )

    for query_config in config['queryConfigs']:
        if query_config['use']:
            for _ in range(query_config['repetition']):
                def pick(d):
                    alt = query_config.get(f"{d}_trip_summary")
                    if alt and dataset_conf.get("trip_summary"):
                        return alt.replace(":trip_summary", dataset_conf["trip_summary"])
                    return query_config[d]
                mobilitydb_sql, postgis_sql, sedona_sql, spacetime_sql, clickhouse_sql, duckdb_sql, values = return_param_values(
                    bbox,
                    lower,
                    upper,
                    pick('mobilitydb'),
                    pick('postgis'),
                    pick('sedona'),
                    query_config['spacetime'],
                    pick('clickhouse'),
                    pick('duckdb'),
                    query_config['parameters'],
                    rnd,
                )
                spacetime_sql = spacetime_units.apply(spacetime_sql, dict(zip(query_config['parameters'], values)), 50.0)

                # FIX (2026-09-30): some mobilitydb: blocks below reference a MobilityDB-native
                # trajectory table via ":trajectory" (distinct from ":dataset", the flat point
                # table every other dialect uses) -- see DATASET_CONFIG's "trajectory_table"
                # comment above. Checked against the RAW template (before any substitution) so
                # this can't be fooled by a substituted parameter value that happens to contain
                # the literal text ":trajectory". When running against geolife (no trajectory
                # table), skip appending this query's mobilitydb task entirely instead of
                # emitting SQL referencing a table that doesn't exist -- every other dialect is
                # unaffected, only mobilitydb_queries.yaml comes out one entry short for those
                # specific queries when generated for geolife.
                # FIX: checked against strip_comment_lines()'s output, not the raw template --
                # a first version of this check matched ":trajectory" inside a "-- ..." comment
                # line explaining *why* a query doesn't use :trajectory (SpatialBBox/kNN above),
                # incorrectly treating those as needing it too and skipping them for geolife.
                needs_trajectory = ":trajectory" in strip_comment_lines(query_config['mobilitydb'] or "")
                skip_mobilitydb = needs_trajectory and trajectory_table is None

                mobilitydb_sql = inject_dataset(mobilitydb_sql, table_name)
                if trajectory_table:
                    # :trajectory_tz first -- ":trajectory" is a prefix of it
                    mobilitydb_sql = substitute_outside_comments(
                        mobilitydb_sql, ":trajectory_tz", f"'{dataset_conf['trajectory_tz']}'")
                    mobilitydb_sql = substitute_outside_comments(mobilitydb_sql, ":trajectory", trajectory_table)
                postgis_sql   = inject_dataset(postgis_sql, table_name)
                sedona_sql    = inject_dataset(sedona_sql, table_name)
                # :id_filter before :id (prefix) -- expands to the dataset's id filter, or vanishes
                def id_filter(sql, col):
                    expr = f"{col} > 0 AND {col} < 1000000000000 AND " if dataset_conf.get("id_filter") else ""
                    return (sql or "").replace(":id_filter AND ", expr)
                mobilitydb_sql = id_filter(mobilitydb_sql, "id")
                postgis_sql    = id_filter(postgis_sql, "id")
                sedona_sql     = id_filter(sedona_sql, "id")
                clickhouse_sql = id_filter(clickhouse_sql, "id")
                duckdb_sql     = id_filter(duckdb_sql, "id")
                spacetime_sql  = id_filter(spacetime_sql, dataset_conf["spacetime_id"])
                # :dataset_trip before inject_dataset -- ":dataset" is a prefix of it
                spacetime_sql = substitute_outside_comments(spacetime_sql, ":dataset_trip", dataset_conf["spacetime_trip_table"])
                spacetime_sql = substitute_outside_comments(spacetime_sql, ":id", dataset_conf["spacetime_id"])
                spacetime_sql = inject_dataset(spacetime_sql, table_name)
                clickhouse_sql = inject_dataset(clickhouse_sql, table_name)
                duckdb_sql    = inject_dataset(duckdb_sql, table_name)

                pattern = r"\['([\d\-: ]+)', '([\d\-: ]+)'\]"
                pattern_single = r"'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})'"
                mobilitydb_sql = re.sub(pattern, convert_to_tstzspan, mobilitydb_sql)
                mobilitydb_sql = re.sub(pattern_single, convert_to_timestamptz, mobilitydb_sql)
                if needs_trajectory and dataset_conf.get("trajectory_tz") == "UTC":
                    mobilitydb_sql = mobilitydb_utc_literals(mobilitydb_sql)
                if not skip_mobilitydb:
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
        content = content.replace('geolife_points', 'tsdb_geolife_points')
        content = content.replace('geolife_trajectories', 'tsdb_geolife_trajectories')
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
    #copy all files to benchmark/client/benchmark_client/src/main/resources/geolife
    resource_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "..", "client", "benchmark_client", "src", "main", "resources", "general")
    shutil.copy("../queries/mobilitydb_queries.yaml", os.path.join(resource_path, "mobilitydb_queries.yaml"))
    shutil.copy("../queries/postgisSQL_queries.yaml", os.path.join(resource_path, "postgisSQL_queries.yaml"))
    shutil.copy("../queries/tsdb_queries.yaml", os.path.join(resource_path, "tsdb_queries.yaml"))
    shutil.copy("../queries/sedonaSQL_queries.yaml", os.path.join(resource_path, "sedonaSQL_queries.yaml"))
    shutil.copy("../queries/spaceTimeSQL_queries.yaml", os.path.join(resource_path, "spaceTimeSQL_queries.yaml"))
    shutil.copy("../queries/clickhouse_queries.yaml", os.path.join(resource_path, "clickhouse_queries.yaml"))
    shutil.copy("../queries/duckdb_queries.yaml", os.path.join(resource_path, "duckdb_queries.yaml"))