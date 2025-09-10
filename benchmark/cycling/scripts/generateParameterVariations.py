import random
from datetime import datetime, timedelta
from typing import List, Any, Dict, Optional
import csv
import os
import yaml
from dataclasses import dataclass
import re

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

def return_param_values(sql1, sql2, sql3, sql4, params, rnd) -> List[Any]:
    parsedSQL1, parsedSQL2, parsedSQL3, parsedSQL4 = sql1, sql2, sql3, sql4
    values = []
    formatter = "%Y-%m-%d %H:%M:%S"

    for param in params:
        replacement = {
            "period_short": lambda: generate_random_time_span(rnd, 2023, 2024, mode=1),
            "period_medium": lambda: generate_random_time_span(rnd, 2023, 2024, mode=2),
            "period_long": lambda: generate_random_time_span(rnd, 2023, 2024, mode=3),
            "period": lambda: generate_random_time_span(rnd, 2023, 2024),
            "instant": lambda: generate_random_timestamp(rnd),
            "day": lambda: get_random_day(rnd, 2023, 2024),
            "university": lambda: get_random_place("universities", rnd),
            "district": lambda: get_random_place("berlin-districts", rnd),
            "point": lambda: get_random_point(rnd, [[6.212909, 52.241256], [8.752841, 50.53438]]),
            "radius": lambda: (rnd.uniform(2.0, 10.0) * 10) / (1000 * 6378),
            "distance": lambda: rnd.randint(1, 10)
        }.get(param, lambda: "")()

        if replacement is not None:
            if isinstance(replacement, str):
                quoted = f"'{replacement}'"
            elif isinstance(replacement, list):
                quoted = ", ".join([f"'{ts}'" for ts in replacement])
                quoted = f"[{quoted}]"
            elif isinstance(replacement, (int, float)):
                quoted = str(replacement)
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

def get_random_point(rnd: random.Random, rectangle: List[List[float]]) -> List[float]:
    upper_left_lon, upper_left_lat = rectangle[0]
    bottom_right_lon, bottom_right_lat = rectangle[1]
    random_lon = upper_left_lon + rnd.random() * (bottom_right_lon - upper_left_lon)
    random_lat = bottom_right_lat + rnd.random() * (upper_left_lat - bottom_right_lat)
    return [random_lon, random_lat]

def get_random_day(rnd: random.Random, start_year: int, end_year: int) -> str:
    start_date = datetime(start_year, 3, 1)
    end_date = datetime(end_year, 3, 31)
    delta = end_date - start_date
    random_day = start_date + timedelta(days=rnd.randint(0, delta.days))
    return random_day.strftime("%Y-%m-%d")

def generate_random_timestamp(rnd: random.Random) -> str:
    year = 2023
    start = datetime(year, 1, 1)
    random_seconds = rnd.randint(0, 365 * 24 * 60 * 60 - 1)
    return (start + timedelta(seconds=random_seconds)).strftime("%Y-%m-%d %H:%M:%S")

def generate_random_time_span(rnd: random.Random, start_year: int, end_year: int, mode: int = 0) -> List[str]:
    formatter = "%Y-%m-%d %H:%M:%S"
    start = datetime(start_year, 3, 1)
    start_seconds = rnd.randint(0, 365 * 24 * 60 * 60)
    timestamp1 = start + timedelta(seconds=start_seconds)

    mode_ranges = {
        1: (0, 2 * 24 * 60 * 60 + 1),
        2: (2 * 24 * 60 * 60, 15 * 24 * 60 * 60 + 1),
        3: (15 * 24 * 60 * 60, 365 * 24 * 60 * 60 + 1)
    }
    seconds_to_shift = rnd.randint(*mode_ranges.get(mode, (0, 365 * 24 * 60 * 60)))
    shift_direction = 1 if rnd.choice([True, False]) else -1
    tentative_end = timestamp1 + timedelta(seconds=shift_direction * seconds_to_shift)

    if tentative_end.year > end_year:
        tentative_end = datetime(end_year, 3, 31, 23, 59, 59)
    elif tentative_end.year < start_year:
        tentative_end = datetime(start_year, 3, 1, 0, 0, 1)

    start_ts, end_ts = sorted([timestamp1, tentative_end])
    return [start_ts.strftime(formatter), end_ts.strftime(formatter)]

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

    for query_config in config['queryConfigs']:
        if query_config['use']:
            for _ in range(query_config['repetition']):
                mobilitydb_sql, postgis_sql, sedona_sql, spacetime_sql, values = return_param_values(
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

    # PostGIS
    with open("../queries/postgisSQL_queries_unprocess.yaml", "w") as f:
        yaml.dump([q.__dict__ for q in postgisSQL_queries], f, sort_keys=False, allow_unicode=True)
    process_file("../queries/postgisSQL_queries_unprocess.yaml", "../queries/postgisSQL_queries.yaml")

    # TSDB variant
    with open("../queries/postgisSQL_queries.yaml", "r") as f:
        content = f.read().replace("ride_points", "tsdb_ride_points")
        with open("../queries/tsdb_queries.yaml", "w") as tsdb_file:
            tsdb_file.write(content)

    # Sedona
    with open("../queries/sedonaSQL_queries.yaml", "w") as f:
        yaml.dump([q.__dict__ for q in sedonaSQL_queries], f, sort_keys=False, allow_unicode=True)

    # Spacetime
    with open("../queries/spaceTimeSQL_queries.yaml", "w") as f:
        yaml.dump([q.__dict__ for q in spaceTimeSQL_queries], f, sort_keys=False, allow_unicode=True)
    with open("../queries/spaceTimeSQL_queries.yaml", "r") as f:
        content = adjust_timestamp_format(f.read())
    with open("../queries/spaceTimeSQL_queries.yaml", "w") as f:
        f.write(content)
