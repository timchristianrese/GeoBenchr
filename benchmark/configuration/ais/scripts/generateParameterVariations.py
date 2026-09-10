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

def get_random_id(rnd):
    return rnd.randint(1_000_000_000, 2_000_000_000)

def generate_bulk_values(rnd, bbox, lower, upper, batch_size=10):
    rows = []
    for _ in range(batch_size):
        point = get_random_point(rnd, bbox)
        ts = generate_random_timestamp_in_bounds(rnd, lower, upper)
        id_val = get_random_id(rnd)
        rows.append(f"({id_val}, {point}, TIMESTAMP '{ts}')")
    #return without single quotes around the values, so that it can be used directly in the SQL statement
    return ", ".join(rows)

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


def return_param_values(bbox, lower, upper, sql1, sql2, sql3, sql4, sql5, sql6, params, rnd) -> List[Any]:
    parsedSQL1, parsedSQL2, parsedSQL3, parsedSQL4 = sql1, sql2, sql3, sql4
    parsedSQL5, parsedSQL6 = sql5, sql6  # clickhouse, duckdb
    values = []
    for param in params:
        replacement = {
            "period_short": lambda: generate_random_time_span_in_bounds(rnd, lower, upper, mode=1),
            "period_medium": lambda: generate_random_time_span_in_bounds(rnd, lower, upper, mode=2),
            "period_long": lambda: generate_random_time_span_in_bounds(rnd, lower, upper, mode=3),
            "period": lambda: generate_random_time_span_in_bounds(rnd, lower, upper),
            "instant": lambda: generate_random_timestamp_in_bounds(rnd, lower, upper),
            "day": lambda: get_random_day(rnd, lower, upper),
            "hour": lambda: get_random_hour(rnd),
            "harbour": lambda: get_random_place("harbours", rnd),
            "region_medium": lambda: generate_random_region_bbox(rnd, bbox, coverage=0.05),
            "region_medium2": lambda: generate_random_region_bbox(rnd, bbox, coverage=0.05),
            "port": lambda: get_random_place("harbours", rnd),
            "island": lambda: get_random_place("islands-wkt-simplified", rnd, ";"),
            "region": lambda: get_random_place("regions-wkt-simplified", rnd, ";"),
            "point": lambda: get_random_point(rnd, bbox),
            "radius": lambda: rnd.randint(1000,5000), #in meters
            "distance": lambda: rnd.randint(1000,5000), #in meters
            "id": lambda: get_random_id(rnd),
            "bulk_values": lambda: generate_bulk_values(rnd, bbox, lower, upper)
        }.get(param, lambda: "")()

  
        if replacement is not None:
            if(param == "bulk_values"):
                quoted = replacement
            elif isinstance(replacement, str):
                # Detect SQL expressions like ST_*, SELECT, or functions with parentheses
                if replacement.strip().upper().startswith(("ST_", "SELECT", "POINT(", "POLYGON(", "LINESTRING(")):
                    quoted = replacement  # leave unquoted (SQL expression)
                else:
                    quoted = f"'{replacement}'"
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

            values.append(replacement)
            parsedSQL1 = substitute_outside_comments(parsedSQL1, f":{param}", quoted)
            parsedSQL2 = substitute_outside_comments(parsedSQL2, f":{param}", quoted)
            parsedSQL3 = substitute_outside_comments(parsedSQL3, f":{param}", quoted)
            parsedSQL4 = substitute_outside_comments(parsedSQL4, f":{param}", quoted)
            parsedSQL5 = substitute_outside_comments(parsedSQL5, f":{param}", quoted)
            parsedSQL6 = substitute_outside_comments(parsedSQL6, f":{param}", quoted)

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
    ts1 = lower + timedelta(seconds=rnd.uniform(0, delta))
    mode_ranges = {
        1: (0, 2*24*60*60),         # short
        2: (2*24*60*60, 15*24*60*60), # medium
        3: (15*24*60*60, delta)       # long
    }
    seconds = rnd.uniform(*mode_ranges.get(mode, (0, delta)))
    ts2 = ts1 + timedelta(seconds=rnd.choice([-1, 1]) * seconds)
    ts2 = max(lower, min(upper, ts2))
    start, end = sorted([ts1, ts2])
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

def get_random_point(rnd, rect):
    ul_lon, ul_lat = rect[0]
    br_lon, br_lat = rect[1]
    lon = ul_lon + rnd.random() * (br_lon - ul_lon)
    lat = br_lat + rnd.random() * (ul_lat - br_lat)
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

# --- Regex helpers ---
def convert_to_tstzspan(m): return f"tstzspan '[{m.group(1)}, {m.group(2)}]'"

def convert_to_tsrange(m): return f"tsrange ('{m.group(1)}'::timestamp, '{m.group(2)}'::timestamp)"

def convert_to_timestamptz(m): return f"timestamptz '[{m.group(1)}]'"
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
                sql1, sql2, sql3, sql4, sql5, sql6, values = return_param_values(bbox, lower, upper, q['mobilitydb'], q['postgis'], q['sedona'], q['spacetime'], q['clickhouse'], q['duckdb'], q['parameters'], rnd)
                sql1 = re.sub(r"\['([\d\-: ]+)', '([\d\-: ]+)'\]", convert_to_tstzspan, sql1)
                sql1 = re.sub(r"'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})'", convert_to_timestamptz, sql1)
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

    #partitioned 
    with open("../queries/mobilitydb_queries_space_partitioned.yaml", "w") as file:
        with open("../queries/mobilitydb_queries.yaml", "r") as original_file:
            content = original_file.read()
            content = content.replace('crossings', 'crossings_adaptgrid')
            file.write(content)

    with open("../queries/mobilitydb_queries_time_partitioned.yaml", "w") as f:
        with open("../queries/mobilitydb_queries.yaml") as original_file:
            content = original_file.read()
            content = content.replace('crossings', 'crossings_adapttimegrid')
            f.write(content)

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

    #copy all created files to /Users/gov/Prog/git/GeoBenchr/benchmark/client/benchmark_client/src/main/resources/ais

    resource_path = "/Users/gov/Prog/git/GeoBenchr/benchmark/client/benchmark_client/src/main/resources/ais"
    shutil.copy("../queries/mobilitydb_queries.yaml", os.path.join(resource_path, "mobilitydb_queries.yaml"))
    shutil.copy("../queries/mobilitydb_queries_space_partitioned.yaml", os.path.join(resource_path, "mobilitydb_queries_space_partitioned.yaml"))
    shutil.copy("../queries/mobilitydb_queries_time_partitioned.yaml", os.path.join(resource_path, "mobilitydb_queries_time_partitioned.yaml"))
    shutil.copy("../queries/postgisSQL_queries.yaml", os.path.join(resource_path, "postgisSQL_queries.yaml"))
    shutil.copy("../queries/tsdb_queries.yaml", os.path.join(resource_path, "tsdb_queries.yaml"))
    shutil.copy("../queries/sedonaSQL_queries.yaml", os.path.join(resource_path, "sedonaSQL_queries.yaml"))
    shutil.copy("../queries/spaceTimeSQL_queries.yaml", os.path.join(resource_path, "spaceTimeSQL_queries.yaml"))
    shutil.copy("../queries/clickhouse_queries.yaml", os.path.join(resource_path, "clickhouse_queries.yaml"))
    shutil.copy("../queries/duckdb_queries.yaml", os.path.join(resource_path, "duckdb_queries.yaml"))