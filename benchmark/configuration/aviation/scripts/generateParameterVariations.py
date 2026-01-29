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

def load_config(path="../config/mobilityDBBenchConf.yaml"):
    try:
        with open(path) as f:
            return yaml.safe_load(f)
    except Exception as e:
        print(f"Failed to load config: {e}")
        return None

def return_param_values(bbox, lower,upper, sql1,sql2, sql3, sql4, params, rnd) -> List[Any]:
    parsedSQL1 = sql1
    parsedSQL2 = sql2
    parsedSQL3 = sql3
    print("SQL3 before processing:", parsedSQL3)
    parsedSQL4 = sql4
    values = []
    formatter = "%Y-%m-%d %H:%M:%S"  # Default formatter for datetime

    for param in params:
        replacement = {
            "period_short": lambda: generate_random_time_span_in_bounds(rnd, lower, upper, mode=1),
            "period_medium": lambda: generate_random_time_span_in_bounds(rnd, lower, upper, mode=2),
            "period_long": lambda: generate_random_time_span_in_bounds(rnd, lower, upper, mode=3),
            "period": lambda: generate_random_time_span_in_bounds(rnd, lower, upper),
            "instant": lambda: generate_random_timestamp_in_bounds(rnd, lower, upper),
            "day": lambda: get_random_day(rnd, lower, upper),
            "city": lambda: get_random_place("cities", rnd),
            "municipality": lambda: get_random_place("municipalities", rnd),
            "county": lambda: get_random_place("counties", rnd),
            "district": lambda: get_random_place("districts", rnd),
            "point": lambda: get_random_point(rnd, bbox),
            "radius": lambda: rnd.randint(1000,5000), #in meters
            "distance": lambda: rnd.randint(1000,5000), #in meters
            "low_altitude": lambda: rnd.randint(50, 150) * 10
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
    return parsedSQL1, parsedSQL2, parsedSQL3, parsedSQL4, values



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

def get_time_bounds(conn, table, time_col="timestamp_col"):
    with conn.cursor() as cur:
        cur.execute(f"SELECT MIN({time_col}), MAX({time_col}) FROM {table};")
        lower, upper = cur.fetchone()
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

    conn = get_db_connection()
    lower, upper = get_time_bounds(conn, "flight_points", "timestamp")
    bbox = get_spatial_bounds(conn, "flight_points", "geom")

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
                mobilitydb_queries.append(QueryTask(
                    name=query_config['name'],
                    type=query_config['type'],
                    sql=LiteralString(mobilitydb_sql), 
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
                    sql=LiteralString(postgis_sql),
                    params=values
                ))

                pattern = r"\['([\d\-: ]+)', '([\d\-: ]+)'\]"
                pattern_single = r"'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})'"
                spacetime_queries.append(QueryTask(
                    name=query_config['name'],
                    type=query_config['type'],
                    sql=LiteralString(spacetime_sql),
                    params=values
                ))

                sedona_sql = fix_between_clause(sedona_sql)
                pattern = r"\['([\d\-: ]+)', '([\d\-: ]+)'\]"
                pattern_single = r"'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})'"
                sedona_queries.append(QueryTask(
                    name=query_config['name'],
                    type=query_config['type'],
                    sql=LiteralString(sedona_sql),
                    params=values
                ))

    if config['benchmark']['mixed']:
        random.shuffle(sedona_queries)

    return mobilitydb_queries, postgis_queries, sedona_queries, spacetime_queries
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


    mobilityDB_queries,postgisSQL_queries, sedonaSQL_queries, spaceTimeSQL_queries = prepare_query_tasks(config, main_random)

    num_queries = len(mobilityDB_queries)
    indices = list(range(num_queries))
    main_random.shuffle(indices)  # use the same seeded RNG

    # Apply the same permutation to all databases
    mobilityDB_queries   = [mobilityDB_queries[i] for i in indices]
    postgisSQL_queries   = [postgisSQL_queries[i] for i in indices]
    sedonaSQL_queries    = [sedonaSQL_queries[i] for i in indices]
    spaceTimeSQL_queries = [spaceTimeSQL_queries[i] for i in indices]

    mobilityDB_data = [query.__dict__ for query in mobilityDB_queries]
    with open("../queries/mobilitydb_queries.yaml", "w") as file:
        yaml.dump(mobilityDB_data, file, sort_keys=False, allow_unicode=True)

    #generate file for partitioned data
    with open("../queries/mobilitydb_queries_space_partitioned.yaml", "w") as file:
        with open("../queries/mobilitydb_queries.yaml", "r") as original_file:
            content = original_file.read()
            content = content.replace('flights', 'flights_adaptgrid')
            file.write(content)
    
    with open("../queries/mobilitydb_queries_time_partitioned.yaml", "w") as file:
        with open("../queries/mobilitydb_queries.yaml", "r") as original_file:
            content = original_file.read()
            content = content.replace('flights', 'flights_adapttimegrid')
            file.write(content)

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
        content = content.replace('SRID=3857;', '').replace("''", "'")  # Remove SRID prefix and fix double single quotes   
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
    #rep
    #copy all files to /Users/gov/Prog/git/GeoBenchr/benchmark/benchmark/client/benchmark_client/src/main/resources/aviation
    resource_path = "/Users/gov/Prog/git/GeoBenchr/benchmark/benchmark/client/benchmark_client/src/main/resources/aviation"
    shutil.copy("../queries/mobilitydb_queries.yaml", os.path.join(resource_path, "mobilitydb_queries.yaml"))
    shutil.copy("../queries/mobilitydb_queries_space_partitioned.yaml", os.path.join(resource_path, "mobilitydb_queries_space_partitioned.yaml"))
    shutil.copy("../queries/mobilitydb_queries_time_partitioned.yaml", os.path.join(resource_path, "mobilitydb_queries_time_partitioned.yaml"))
    shutil.copy("../queries/postgisSQL_queries.yaml", os.path.join(resource_path, "postgisSQL_queries.yaml"))
    shutil.copy("../queries/tsdb_queries.yaml", os.path.join(resource_path, "tsdb_queries.yaml"))
    shutil.copy("../queries/sedonaSQL_queries.yaml", os.path.join(resource_path, "sedonaSQL_queries.yaml"))
    shutil.copy("../queries/spaceTimeSQL_queries.yaml", os.path.join(resource_path, "spaceTimeSQL_queries.yaml"))