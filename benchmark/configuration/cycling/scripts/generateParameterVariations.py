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

def get_db_connection():
    return psycopg2.connect(
        host="localhost",
        port=5433,
        dbname="postgres",
        user="postgres",
        password="test"
    )

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

def return_param_values(bbox, lower, upper, sql1, sql2, sql3, sql4, params, rnd) -> List[Any]:
    parsedSQL1, parsedSQL2, parsedSQL3, parsedSQL4 = sql1, sql2, sql3, sql4
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
            "distance": lambda: rnd.randint(100,1000)
        }.get(param, lambda: "")()

        if replacement is not None:
            if isinstance(replacement, str):
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

            values.append(replacement)
            parsedSQL1 = parsedSQL1.replace(f":{param}", quoted)
            parsedSQL2 = parsedSQL2.replace(f":{param}", quoted)
            parsedSQL3 = parsedSQL3.replace(f":{param}", quoted)
            parsedSQL4 = parsedSQL4.replace(f":{param}", quoted)

    return parsedSQL1, parsedSQL2, parsedSQL3, parsedSQL4, values


# -----------------
# Helpers
# -----------------

def get_spatial_bounds(conn, table, geom_col="geom"):
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT
                ST_XMin(env),
                ST_YMin(env),
                ST_XMax(env),
                ST_YMax(env)
            FROM (
                SELECT ST_Envelope(ST_Extent({geom_col}::geometry)) AS env
                FROM {table}
            ) AS sub;
        """)
        xmin, ymin, xmax, ymax = cur.fetchone()
    # same format as before: [[ul_lon, ul_lat], [br_lon, br_lat]]
    return [[xmin, ymax], [xmax, ymin]]

def get_random_point(rnd, rect):
    ul_lon, ul_lat = rect[0]
    br_lon, br_lat = rect[1]
    lon = ul_lon + rnd.random() * (br_lon - ul_lon)
    lat = br_lat + rnd.random() * (ul_lat - br_lat)
    return f"ST_SetSRID(ST_Point({lon}, {lat}), 3857)"



def get_time_bounds(conn, table, time_col="timestamp_col"):
    with conn.cursor() as cur:
        cur.execute(f"SELECT MIN({time_col}), MAX({time_col}) FROM {table};")
        lower, upper = cur.fetchone()
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
    conn = get_db_connection()
    bbox = get_spatial_bounds(conn, "ride_points", "geom")
    lower, upper = get_time_bounds(conn, "ride_points", "timestamp")
    for query_config in config['queryConfigs']:
        if query_config['use']:
            for _ in range(query_config['repetition']):
                mobilitydb_sql, postgis_sql, sedona_sql, spacetime_sql, values = return_param_values(
                    bbox,
                    lower,
                    upper,
                    query_config['mobilitydb'],
                    query_config['postgis'],
                    query_config['sedona'],
                    query_config['spacetime'],
                    query_config['parameters'],
                    rnd,
                )

                pattern = r"\['([\d\-: ]+)', '([\d\-: ]+)'\]"
                pattern_single = r"'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})'"

                mobilitydb_sql = re.sub(pattern, convert_to_tstzspan, mobilitydb_sql)
                mobilitydb_sql = re.sub(pattern_single, convert_to_timestamptz, mobilitydb_sql)
                mobilitydb_queries.append(QueryTask(query_config['name'], query_config['type'], LiteralString(mobilitydb_sql), values))

                postgis_sql = re.sub(pattern,convert_to_tsrange, postgis_sql)
                postgis_sql = re.sub(pattern_single, convert_to_timestamptz, postgis_sql)
                postgis_queries.append(QueryTask(query_config['name'], query_config['type'], LiteralString(postgis_sql), values))
                sedona_queries.append(QueryTask(query_config['name'], query_config['type'], LiteralString(fix_between_clause(sedona_sql)), values))
                spacetime_queries.append(QueryTask(query_config['name'], query_config['type'], LiteralString(spacetime_sql), values))

    if config['benchmark']['mixed']:
        random.shuffle(sedona_queries)

    return mobilitydb_queries, postgis_queries, sedona_queries, spacetime_queries


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

    mobilityDB_queries, postgisSQL_queries, sedonaSQL_queries, spaceTimeSQL_queries = prepare_query_tasks(config, main_random)

    num_queries = len(mobilityDB_queries)
    indices = list(range(num_queries))
    main_random.shuffle(indices)

    mobilityDB_queries   = [mobilityDB_queries[i] for i in indices]
    postgisSQL_queries   = [postgisSQL_queries[i] for i in indices]
    sedonaSQL_queries    = [sedonaSQL_queries[i] for i in indices]
    spaceTimeSQL_queries = [spaceTimeSQL_queries[i] for i in indices]

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

    #copy all created files to /Users/gov/Prog/git/GeoBenchr/benchmark/benchmark/client/benchmark_client/src/main/resources/cycling

    resource_path = "/Users/gov/Prog/git/GeoBenchr/benchmark/benchmark/client/benchmark_client/src/main/resources/cycling"
    shutil.copy("../queries/mobilitydb_queries.yaml", os.path.join(resource_path, "mobilitydb_queries.yaml"))
    shutil.copy("../queries/mobilitydb_queries_space_partitioned.yaml", os.path.join(resource_path, "mobilitydb_queries_space_partitioned.yaml"))
    shutil.copy("../queries/mobilitydb_queries_time_partitioned.yaml", os.path.join(resource_path, "mobilitydb_queries_time_partitioned.yaml"))
    shutil.copy("../queries/postgisSQL_queries.yaml", os.path.join(resource_path, "postgisSQL_queries.yaml"))  
    shutil.copy("../queries/tsdb_queries.yaml", os.path.join(resource_path, "tsdb_queries.yaml"))
    shutil.copy("../queries/sedonaSQL_queries.yaml", os.path.join(resource_path, "sedonaSQL_queries.yaml"))
    shutil.copy("../queries/spaceTimeSQL_queries.yaml", os.path.join(resource_path, "spaceTimeSQL_queries.yaml"))