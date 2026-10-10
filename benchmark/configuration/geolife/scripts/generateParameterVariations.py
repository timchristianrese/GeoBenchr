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

    Each live entry is an (id, timestamp) pair, not just an id: geolife_points has no per-row
    primary key, so "id" alone identifies a user's trace, shared by every point of it, not a
    single point. The *_single update/delete queries need the exact timestamp their scratch row
    was inserted with too, so pick_existing_pair/pop_existing_pair hand back both -- see
    updategeolifePointSingle/deletegeolifePointSingle in combinedBenchConf.yaml.
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

def generate_random_venue(rnd):
    # geolife_points' venue_id/venue_type describe a POI a point sits at; venue_id == 0 (and an
    # empty venue_type) is the dataset's own convention for "not at a venue" (see the
    # "venue_id <> 0" filters in SlidingWindowActivityInRegionPerMinuteWithNonZeroID and friends
    # in combinedBenchConf.yaml) -- the correct default for a freshly-generated scratch point,
    # which isn't tied to any real POI.
    return 0, ""

def generate_bulk_values(rnd, bbox, lower, upper, batch_size=10):
    # Returns (postgis_rows, clickhouse_rows): the same id/timestamp/location/venue per row,
    # formatted for each dialect's real columns -- geolife_points has "geom" in postgis/
    # mobilitydb/duckdb but only plain "lat"/"lon" columns in ClickHouse (see
    # insertSinglegeolifePoint's FIX note in combinedBenchConf.yaml).
    # FIX: rows didn't include "venue_id"/"venue_type" -- geolife_points has those columns (see
    # insertSinglegeolifePoint's FIX note) that every row needs.
    postgis_rows = []
    clickhouse_rows = []
    for _ in range(batch_size):
        lon, lat = get_random_lonlat(rnd, bbox)
        ts = generate_random_timestamp_in_bounds(rnd, lower, upper)
        venue_id, venue_type = generate_random_venue(rnd)
        id_val = _SCRATCH_IDS.mint(ts)
        postgis_rows.append(f"({id_val}, ST_SetSRID(ST_Point({lon}, {lat}), 3857), TIMESTAMP '{ts}', {venue_id}, '{venue_type}')")
        clickhouse_rows.append(f"({id_val}, TIMESTAMP '{ts}', {venue_id}, '{venue_type}', {lat}, {lon})")
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


def spacetime_region_literal(value):
    """FIX (2026-10-02): SpaceTime takes regions as a lon/lat WKT string cast to gpolygon; the shared
    rendering is PostGIS-only (ST_Transform(ST_SetSRID(ST_MakeEnvelope(...)))), which SpaceTime
    rejected ("unknown function: st_makeenvelope") on every :region query in this scenario."""
    num = r"\s*(-?[\d.]+)\s*"
    env = re.search(rf"ST_MakeEnvelope\({num},{num},{num},{num}", value) if isinstance(value, str) else None
    if not env:
        return value
    x1, y1, x2, y2 = env.groups()
    return f"'POLYGON (({x1} {y1}, {x2} {y1}, {x2} {y2}, {x1} {y2}, {x1} {y1}))'"

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
            # generate_random_region_bbox() returns one PostGIS
            # ST_Transform(ST_SetSRID(ST_MakeEnvelope(...))) string shared verbatim across all 6
            # dialects' templates -- fine for postgis/sedona/duckdb, but ClickHouse has none of
            # those functions (Code 46 UNKNOWN_FUNCTION, confirmed live). Decomposed into
            # ClickHouse-specific :region_X_lon_min/_lat_min/_lon_max/_lat_max tokens further
            # below (same pattern as :point_lon/:point_lat) -- combinedBenchConf.yaml's
            # clickhouse: templates for CountPOIsByTypeInRegion/
            # SlidingWindowActivityInRegionPerMinuteWithNonZeroID/UniqueUsersPerHourInRegionInPeriod
            # use those tokens directly now instead of the shared PostGIS expression. See
            # generate_random_region_bbox()'s own FIX comment for the separate lat/lon-axis bug
            # this also fixes (get_spatial_bounds() was fine; the mismatch was entirely in how
            # this function unpacked its return value).
            "region_small": lambda: generate_random_region_bbox(rnd, bbox, coverage=0.02),
            "region_medium": lambda: generate_random_region_bbox(rnd, bbox, coverage=0.05),
            "region_medium2": lambda: generate_random_region_bbox(rnd, bbox, coverage=0.05),
            "region_large": lambda: generate_random_region_bbox(rnd, bbox, coverage=0.08),
            "period": lambda: generate_random_time_span_in_bounds(rnd, lower, upper),
            "instant": lambda: shared_instant if shared_instant is not None else generate_random_timestamp_in_bounds(rnd, lower, upper),
            "day": lambda: get_random_day(rnd, lower, upper),
            "city": lambda: get_random_place("cities", rnd),
            "municipality": lambda: get_random_place("municipalities", rnd),
            "county": lambda: get_random_place("counties", rnd),
            "district": lambda: get_random_place("districts", rnd),
            "point": lambda: get_random_point(rnd, bbox),
            "radius": lambda: rnd.randint(1000,2000), #in meters
            "distance": lambda: rnd.randint(1000,2000), #in meters
            "low_altitude": lambda: rnd.randint(50, 150) * 10,
            "time_threshold": lambda: rnd.randint(1, 10) * 60,  # in seconds, so between 1 and 10 minutes
            "distance_threshold": lambda: rnd.randint(100, 500) * 10, # in meters, so between 1000 and 5000 meters
            "venue_id": lambda: generate_random_venue(rnd)[0],
            "venue_type": lambda: generate_random_venue(rnd)[1],
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

            # ClickHouse has no ST_MakeEnvelope/ST_SetSRID/ST_Transform (PostGIS-only, Code 46
            # UNKNOWN_FUNCTION, confirmed live) -- a couple of queries decompose ":region_X" into
            # ":region_X_lon_min"/"_lat_min"/"_lon_max"/"_lat_max" scalars for that dialect only,
            # for a plain "lon BETWEEN ... AND lat BETWEEN ..." bbox check instead (see the FIX
            # comments on those clickhouse: blocks in combinedBenchConf.yaml). Keyed by the actual
            # param name (region_small/region_medium/region_medium2/region_large) rather than a
            # fixed name, since a query could in principle use more than one region_* param.
            # Must run before the generic ":region_X" replace below for the same reason as :point
            # above. generate_random_region_bbox() always returns
            # "ST_Transform(ST_SetSRID(ST_MakeEnvelope(lon_min, lat_min, lon_max, lat_max), ...";
            # extracting those 4 numbers here rather than re-deriving them keeps this in sync with
            # that function's actual output instead of duplicating its bbox math.
            if param.startswith("region") and isinstance(replacement, str):
                envelope_coords = re.search(
                    r"ST_MakeEnvelope\(\s*([-\d.]+)\s*,\s*([-\d.]+)\s*,\s*([-\d.]+)\s*,\s*([-\d.]+)\s*\)",
                    replacement
                )
                if envelope_coords:
                    lon_min, lat_min, lon_max, lat_max = envelope_coords.groups()
                    parsedSQL5 = substitute_outside_comments(parsedSQL5, f":{param}_lon_min", lon_min)
                    parsedSQL5 = substitute_outside_comments(parsedSQL5, f":{param}_lat_min", lat_min)
                    parsedSQL5 = substitute_outside_comments(parsedSQL5, f":{param}_lon_max", lon_max)
                    parsedSQL5 = substitute_outside_comments(parsedSQL5, f":{param}_lat_max", lat_max)

                    # FIX: confirmed live -- Sedona/GeoTools' ST_Transform rejects a bare numeric
                    # SRID code as its target-CRS argument ("First failed to read as a well-known
                    # CRS code... No authority was defined for code '3857'"), unlike PostGIS which
                    # accepts either form. ST_SetSRID's own (source) argument is untouched --
                    # only the target arg errored live, so only that one needs the 'EPSG:' prefix.
                    parsedSQL3 = substitute_outside_comments(
                        parsedSQL3, f":{param}",
                        f"ST_Transform(ST_SetSRID(ST_MakeEnvelope({lon_min}, {lat_min}, {lon_max}, {lat_max}), 4326), 'EPSG:3857')"
                    )
                    # FIX: confirmed live -- DuckDB's spatial extension has no ST_SetSRID at all
                    # ("Scalar Function with name st_setsrid does not exist!"); its geometries
                    # carry no SRID metadata, so ST_Transform takes the source/target CRS as
                    # explicit string arguments instead (ST_Transform(geom, source_crs,
                    # target_crs[, always_xy])). Also needs always_xy=true -- EPSG:4326's default
                    # axis order is [lat, lon], but ST_MakeEnvelope's args here are [lon, lat]
                    # (confirmed live: omitting always_xy silently produces "POLYGON((inf inf, ...))").
                    parsedSQL6 = substitute_outside_comments(
                        parsedSQL6, f":{param}",
                        f"ST_Transform(ST_MakeEnvelope({lon_min}, {lat_min}, {lon_max}, {lat_max}), 'EPSG:4326', 'EPSG:3857', true)"
                    )

            # FIX: replacement is now a (postgis_rows, clickhouse_rows) tuple for "bulk_values" --
            # PyYAML would serialize a bare tuple with its own "!!python/tuple" tag, which Java's
            # SnakeYAML (the client's actual YAML reader) doesn't recognize (confirmed live for the
            # equivalent aviation/scripts case). params: is only ever informational documentation
            # of what was substituted (not read back by the client), so the postgis-shaped half is
            # fine here.
            values.append(quoted if param == "bulk_values" else replacement)
            parsedSQL1 = substitute_outside_comments(parsedSQL1, f":{param}", quoted)
            parsedSQL2 = substitute_outside_comments(parsedSQL2, f":{param}", quoted)
            parsedSQL3 = substitute_outside_comments(parsedSQL3, f":{param}", quoted)
            if param.startswith("period"):
                span_match = re.match(r"'([^']+)' AND '([^']+)'", replacement) if isinstance(replacement, str) else None
                if span_match:
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
                    parsedSQL4 = substitute_outside_comments(parsedSQL4, f":{param}", quoted)
            elif param == "instant":
                # FIX: confirmed live -- a bare quoted timestamp in SpaceTime arithmetic (e.g.
                # CityActivityAtInstant's "(:instant - TP '10s')") fails ("no matching overload
                # for: -"); needs the same explicit type prefix TP already needs for durations
                # ("TS 'ts' - TP '10s'" succeeds).
                parsedSQL4 = substitute_outside_comments(parsedSQL4, f":{param}", f"TS {quoted}")
            elif param.startswith("region"):
                parsedSQL4 = substitute_outside_comments(parsedSQL4, f":{param}", spacetime_region_literal(quoted))
            elif param == "point" and isinstance(replacement, str) and re.search(r"ST_Point\(", replacement):
                # SpaceTime takes the point as lon/lat WKT (cast to gpoint by the template)
                pt = re.search(r"ST_Point\(\s*(-?[\d.]+)\s*,\s*(-?[\d.]+)\s*\)", replacement)
                parsedSQL4 = substitute_outside_comments(parsedSQL4, f":{param}", f"'POINT ({pt.group(1)} {pt.group(2)})'")
            else:
                parsedSQL4 = substitute_outside_comments(parsedSQL4, f":{param}", quoted)
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

    bbox format (matches get_spatial_bounds()'s actual return shape and get_random_lonlat()'s,
    which already consumes the same object correctly -- this function used to assume
    (min_lat, min_lon), (max_lat, max_lon) instead, silently scrambling which axis got which
    span/bound; see the FIX below):
        [[upper_left_lon, upper_left_lat], [bottom_right_lon, bottom_right_lat]]

    coverage:
        0.02 -> 2%
        0.05 -> 5%
        0.08 -> 8%
    """

    # FIX: was "min_lat, min_lon = bbox[0]" / "max_lat, max_lon = bbox[1]", assuming bbox held
    # (lat, lon) mins/maxes -- but bbox is actually [[ul_lon, ul_lat], [br_lon, br_lat]] (same
    # object get_random_lonlat() already consumes correctly, a few lines below). That mismatch
    # mislabeled both which axis a value belonged to (lat vs lon) AND its min/max role (ul_lat is
    # the NORTH/max latitude, not a "min"). Confirmed live: query params already generated from
    # this bug have (lat, lon, lat, lon) magnitudes that don't match what the code's own variable
    # names/ST_MakeEnvelope(start_lon, start_lat, ...) call claimed. Fixed by unpacking the bbox
    # in the shape it's actually in, then deriving min/max explicitly (upper-left = north/west =
    # max lat, min lon; bottom-right = south/east = min lat, max lon).
    ul_lon, ul_lat = bbox[0]
    br_lon, br_lat = bbox[1]
    min_lat, max_lat = br_lat, ul_lat
    min_lon, max_lon = ul_lon, br_lon

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

    # FIX: was returning a raw (start_lat, start_lon, end_lat, end_lon) tuple -- substituted
    # into SQL via str(replacement), that produced a bare "(52.1, 8.2, 52.3, 8.4)" literal
    # wherever a query does e.g. "ST_Intersects(geom, :region_medium)" (not valid PostGIS syntax),
    # and yaml.dump serialized the same raw tuple into the "params:" field using a Python-specific
    # "!!python/tuple" tag that a standard (non-Python) YAML loader -- including SnakeYAML in the
    # Java benchmark_client -- cannot parse, breaking the whole output file. Matches the working
    # ST_MakeEnvelope-based implementation already used by the other four datasets' scripts.
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
    return lower, upper


def generate_random_timestamp_in_bounds(rnd, lower, upper):
    delta = (upper - lower).total_seconds()
    ts = lower + timedelta(seconds=rnd.uniform(0, delta))
    return ts.strftime("%Y-%m-%d %H:%M:%S")

def generate_random_time_span_in_bounds(rnd, lower, upper, mode=0):
    delta = (upper - lower).total_seconds()
    ts1 = lower + timedelta(seconds=rnd.uniform(0, delta))
    mode_ranges = {
        1: (0, 2*24*60*60),         # short
        2: (2*24*60*60, 4*24*60*60), # medium
        3: (4*24*60*60, 8*24*60*60)   # long
    }
    seconds = rnd.uniform(*mode_ranges.get(mode, (0, delta)))
    ts2 = ts1 + timedelta(seconds=rnd.choice([-1, 1]) * seconds)
    ts2 = max(lower, min(upper, ts2))
    start, end = sorted([ts1, ts2])
    return f"'{start.strftime('%Y-%m-%d %H:%M:%S')}' AND '{end.strftime('%Y-%m-%d %H:%M:%S')}'"

# --- Spatial bounds ---
def get_spatial_bounds(table, geom_col="geom"):
    #check if file exists with the spatial bounds, if yes, read from file and return
    if os.path.exists("spatial_bounds.txt"):
        with open("spatial_bounds.txt", "r") as f:
            bounds = f.read().strip().split(",")
            # NOT buggy despite looking asymmetric: spatial_bounds.txt is xmin,ymin,xmax,ymax
            # (lon_min,lat_min,lon_max,lat_max -- see the commented-out ST_XMin/ST_YMin/ST_XMax/
            # ST_YMax query below), and this returns [[lon_min, lat_max], [lon_max, lat_min]] =
            # [[upper_left_lon, upper_left_lat], [bottom_right_lon, bottom_right_lat]] -- exactly
            # what get_random_lonlat() below already expects and consumes correctly. The actual
            # bug was in generate_random_region_bbox() assuming a different, incompatible shape
            # from this same return value -- fixed there instead of here (see its own FIX
            # comment) so get_random_lonlat() (already correct) isn't disturbed.
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

def get_random_lonlat(rnd, rect):
    ul_lon, ul_lat = rect[0]
    br_lon, br_lat = rect[1]
    lon = ul_lon + rnd.random() * (br_lon - ul_lon)
    lat = br_lat + rnd.random() * (ul_lat - br_lat)
    return lon, lat

def get_random_point(rnd, rect):
    # FIX (2026-10-02): was ST_SetSRID(ST_Point(lon, lat), 3857) -- degrees labelled as Web
    # Mercator meters, i.e. a point ~100 m from (0, 0), so ClosestRestaurantToPoint searched off the
    # coast of Africa in PostGIS/DuckDB/Sedona and (after the 3857->4326 back-conversion) SpaceTime,
    # and returned 0 rows everywhere. Now the same projected form the general generator uses.
    lon, lat = get_random_lonlat(rnd, rect)
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






def prepare_query_tasks(config, rnd) -> List[QueryTask]:
    mobilitydb_queries = []
    postgis_queries = []
    sedona_queries = []
    spacetime_queries = []
    clickhouse_queries = []
    duckdb_queries = []

    lower, upper = get_time_bounds("geolife_points", "timestamp")
    bbox = get_spatial_bounds("geolife_points", "geom")

    for query_config in config['queryConfigs']:
        if query_config['use']:
            for _ in range(query_config['repetition']):
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
                spacetime_sql = spacetime_units.apply(spacetime_sql, dict(zip(query_config['parameters'], values)), 39.95)
                pattern = r"\['([\d\-: ]+)', '([\d\-: ]+)'\]"
                pattern_single = r"'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})'"
                mobilitydb_sql = re.sub(pattern, convert_to_tstzspan, mobilitydb_sql)
                mobilitydb_sql = re.sub(pattern_single, convert_to_timestamptz, mobilitydb_sql)
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
    #copy all files to /Users/gov/Prog/git/GeoBenchr/benchmark/client/benchmark_client/src/main/resources/geolife
    resource_path = "/Users/gov/Prog/git/GeoBenchr/benchmark/client/benchmark_client/src/main/resources/geolife"
    shutil.copy("../queries/mobilitydb_queries.yaml", os.path.join(resource_path, "mobilitydb_queries.yaml"))
    shutil.copy("../queries/postgisSQL_queries.yaml", os.path.join(resource_path, "postgisSQL_queries.yaml"))
    shutil.copy("../queries/tsdb_queries.yaml", os.path.join(resource_path, "tsdb_queries.yaml"))
    sedona_crs.fix_file("../queries/sedonaSQL_queries.yaml")  # explicit CRS strings for ST_Transform (scripts/sedona_crs.py)
    shutil.copy("../queries/sedonaSQL_queries.yaml", os.path.join(resource_path, "sedonaSQL_queries.yaml"))
    shutil.copy("../queries/spaceTimeSQL_queries.yaml", os.path.join(resource_path, "spaceTimeSQL_queries.yaml"))
    shutil.copy("../queries/clickhouse_queries.yaml", os.path.join(resource_path, "clickhouse_queries.yaml"))
    shutil.copy("../queries/duckdb_queries.yaml", os.path.join(resource_path, "duckdb_queries.yaml"))