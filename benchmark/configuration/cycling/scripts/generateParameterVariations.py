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
# Example usage:
# convert_st_points_in_yaml("queries.yaml"

class LiteralString(str): pass

def get_random_id(rnd):
    return rnd.randint(1_000_000_000, 2_000_000_000)

def generate_bulk_values(rnd, bbox, lower, upper, batch_size=10):
    rows = []
    for _ in range(batch_size):
        point = get_random_point(rnd, bbox)
        ts = generate_random_timestamp_in_bounds(rnd, lower, upper)
        id_val = get_random_id(rnd)
        rows.append(f"({id_val}, {point}, TIMESTAMP '{ts}')")
    return ", ".join(rows)



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


def return_param_values(bbox, lower, upper, sql1, sql2, sql3, sql4, sql5, sql6, params, rnd) -> List[Any]:
    parsedSQL1, parsedSQL2, parsedSQL3, parsedSQL4 = sql1, sql2, sql3, sql4
    parsedSQL5, parsedSQL6 = sql5, sql6  # clickhouse, duckdb
    values = []
    formatter = "%Y-%m-%d %H:%M:%S"

    for param in params:
        replacement = {
            "period_short": lambda: generate_random_time_span_in_bounds(rnd, lower, upper, mode=1),
            "period_medium": lambda: generate_random_time_span_in_bounds(rnd, lower, upper, mode=2),
            "period_long": lambda: generate_random_time_span_in_bounds(rnd, lower, upper, mode=3),
            "period": lambda: generate_random_time_span_in_bounds(rnd, lower, upper),
            "instant": lambda: generate_random_timestamp_in_bounds(rnd, lower, upper),
            "day": lambda: get_random_day(rnd, lower, upper),
            "university": lambda: get_random_place("universities", rnd),
            "district": lambda: get_random_place("berlin-districts", rnd),
            "point": lambda: get_random_point(rnd, [[6.212909, 52.241256], [8.752841, 50.53438]]),
            "radius": lambda: rnd.randint(100,1000), #in meters
            "distance": lambda: rnd.randint(100,1000),
            "id": lambda: get_random_id(rnd),
            "bulk_values": lambda: generate_bulk_values(rnd, bbox, lower, upper, batch_size=10)
        }.get(param, lambda: "")()

        if replacement is not None:
            if param == "bulk_values":
                quoted = replacement
            elif isinstance(replacement, str):
                # Detect SQL expressions like ST_*, SELECT, or functions with parentheses
                if replacement.strip().upper().startswith(("ST_", "SELECT", "POINT(", "POLYGON(", "LINESTRING(")):
                    quoted = replacement  # leave unquoted (SQL expression)
                else:
                    quoted = f"'{replacement}'"
           
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

            values.append(replacement)
            parsedSQL1 = substitute_outside_comments(parsedSQL1, f":{param}", quoted)
            parsedSQL2 = substitute_outside_comments(parsedSQL2, f":{param}", quoted)
            parsedSQL3 = substitute_outside_comments(parsedSQL3, f":{param}", quoted)
            parsedSQL4 = substitute_outside_comments(parsedSQL4, f":{param}", quoted)
            parsedSQL5 = substitute_outside_comments(parsedSQL5, f":{param}", quoted)
            parsedSQL6 = substitute_outside_comments(parsedSQL6, f":{param}", quoted)

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

def get_random_point(rnd, rect):
    ul_lon, ul_lat = rect[0]
    br_lon, br_lat = rect[1]

    lon = ul_lon + rnd.random() * (br_lon - ul_lon)
    lat = br_lat + rnd.random() * (ul_lat - br_lat)

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
    ts = lower + timedelta(seconds=rnd.uniform(0, delta))
    return ts.strftime("%Y-%m-%d %H:%M:%S")

def generate_random_time_span_in_bounds(rnd, lower, upper, mode=0):
    delta = (upper - lower).total_seconds()
    ts1 = lower + timedelta(seconds=rnd.uniform(0, delta))
    mode_ranges = {
        1: (30 * 60, 2 * 60 * 60),          # short
        2: (2*24*60*60, 15*24*60*60), # medium
        3: (15*24*60*60, delta)       # long
    }
    seconds = rnd.uniform(*mode_ranges.get(mode, (0, delta)))
    ts2 = ts1 + timedelta(seconds=rnd.choice([-1, 1]) * seconds)
    ts2 = max(lower, min(upper, ts2))
    start, end = sorted([ts1, ts2])
    return f"'{start.strftime('%Y-%m-%d %H:%M:%S')}' AND '{end.strftime('%Y-%m-%d %H:%M:%S')}'"


def get_random_place(filename: str, rnd: random.Random) -> Optional[str]:
    filepath = os.path.join("..", "data", f"{filename}.csv")
    try:
        with open(filepath, newline='', encoding='utf-8') as csvfile:
            reader = list(csv.reader(csvfile))
            if len(reader) <= 1: return None
            chosen_row = rnd.choice(reader[1:])
            return chosen_row[0]
    except Exception as e:
        print(f"Error reading {filepath}: {e}")
        return None


# -----------------
# Regex helpers
# -----------------

def convert_to_tstzspan(match): return f"tstzspan '[{match.group(1)}, {match.group(2)}]'"

def convert_to_tsrange(match): return f"tsrange '[{match.group(1)}, {match.group(2)}]'"

def convert_to_timestamptz(match): return f"timestamptz '[{match.group(1)}]'"

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
                )

                pattern = r"\['([\d\-: ]+)', '([\d\-: ]+)'\]"
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

    #partitioned 
    with open("../queries/mobilitydb_queries_space_partitioned.yaml", "w") as file:
        with open("../queries/mobilitydb_queries.yaml", "r") as original_file:
            content = original_file.read()
            content = content.replace('rides', 'rides_adaptgrid')
            file.write(content)
    with open("../queries/mobilitydb_queries_time_partitioned.yaml", "w") as f:
        with open("../queries/mobilitydb_queries.yaml", "r") as original_file:
            content = original_file.read()
            content = content.replace('rides', 'rides_adapttimegrid')
            f.write(content)
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

    #copy all created files to /Users/gov/Prog/git/GeoBenchr/benchmark/client/benchmark_client/src/main/resources/cycling

    resource_path = "/Users/gov/Prog/git/GeoBenchr/benchmark/client/benchmark_client/src/main/resources/cycling"
    shutil.copy("../queries/mobilitydb_queries.yaml", os.path.join(resource_path, "mobilitydb_queries.yaml"))
    shutil.copy("../queries/mobilitydb_queries_space_partitioned.yaml", os.path.join(resource_path, "mobilitydb_queries_space_partitioned.yaml"))
    shutil.copy("../queries/mobilitydb_queries_time_partitioned.yaml", os.path.join(resource_path, "mobilitydb_queries_time_partitioned.yaml"))
    shutil.copy("../queries/postgisSQL_queries.yaml", os.path.join(resource_path, "postgisSQL_queries.yaml"))
    shutil.copy("../queries/tsdb_queries.yaml", os.path.join(resource_path, "tsdb_queries.yaml"))
    shutil.copy("../queries/sedonaSQL_queries.yaml", os.path.join(resource_path, "sedonaSQL_queries.yaml"))
    shutil.copy("../queries/spaceTimeSQL_queries.yaml", os.path.join(resource_path, "spaceTimeSQL_queries.yaml"))
    shutil.copy("../queries/clickhouse_queries.yaml", os.path.join(resource_path, "clickhouse_queries.yaml"))
    shutil.copy("../queries/duckdb_queries.yaml", os.path.join(resource_path, "duckdb_queries.yaml"))