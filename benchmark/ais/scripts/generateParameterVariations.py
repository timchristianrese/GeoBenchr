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

def return_param_values(sql, params, rnd) -> List[Any]:
    parsedSQL = sql
    values = []
    formatter = "%Y-%m-%d %H:%M:%S"  # Default formatter for datetime

    for param in params:
        replacement = {
            "period_short": lambda: generate_random_time_span(rnd, 2019, mode=1),
            "period_medium": lambda: generate_random_time_span(rnd, 2019, mode=2),
            "period_long": lambda: generate_random_time_span(rnd, 2019, mode=3),
            "period": lambda: generate_random_time_span(rnd, 2019),
            "instant": lambda: generate_random_timestamp(rnd),
            "day": lambda: get_random_day(rnd, 2019),
            "hour": lambda: get_random_hour(rnd),
            "harbour": lambda: get_random_place("harbours", rnd),
            "harbour2": lambda: get_random_place("harbours", rnd),
            "island": lambda: get_random_place("islands-wkt", rnd),
            "region": lambda: get_random_place("regions-wkt", rnd),
            "point": lambda: get_random_point(rnd, [[6.212909, 52.241256], [8.752841, 50.53438]]),
            "radius": lambda: (rnd.uniform(2.0, 10.0) * 10) / (1000 * 6378),
            "low_altitude": lambda: rnd.randint(50, 150) * 10,
            "distance": lambda: rnd.randint(1, 10)
        }.get(param, lambda: "")()

        if replacement is not None:
            # Quote strings and timestamps
            if isinstance(replacement, str):
                quoted = f"'{replacement}'"
            elif isinstance(replacement, list):
                # Handle time periods (start, end timestamps)
                quoted = ", ".join([f"'{ts}'" for ts in replacement])
                quoted = f"[{quoted}]"  # For array-style SQL functions like attime()
            elif isinstance(replacement, (int, float)):
                quoted = str(replacement)
            elif isinstance(replacement, list) and all(isinstance(x, (int, float)) for x in replacement):
                # coordinates or similar
                quoted = f"ARRAY{replacement}"
            else:
                quoted = str(replacement)

            values.append(replacement)
            parsedSQL = parsedSQL.replace(f":{param}", quoted)

    return parsedSQL, values


def get_random_point(rnd: random.Random, rectangle: List[List[float]]) -> List[float]:
    upper_left_lon, upper_left_lat = rectangle[0]
    bottom_right_lon, bottom_right_lat = rectangle[1]

    random_lon = upper_left_lon + rnd.random() * (bottom_right_lon - upper_left_lon)
    random_lat = bottom_right_lat + rnd.random() * (upper_left_lat - bottom_right_lat)

    return [random_lon, random_lat]

def get_random_day(rnd: random.Random, year: int) -> str:
    formatter = "%Y-%m-%d %H:%M:%S"
    start_date = datetime(year, 1, 1)
    end_date = datetime(year, 12, 31)
    delta = end_date - start_date
    random_day = start_date + timedelta(days=rnd.randint(0, delta.days))
    return random_day.strftime("%Y-%m-%d")

def get_random_hour(rnd: random.Random) -> str:
    return rnd.randint(0,23)

def generate_random_timestamp(rnd: random.Random) -> str:
    year = 2023
    formatter = "%Y-%m-%d %H:%M:%S"
    start = datetime(year, 1, 1)
    random_seconds = rnd.randint(0, 365 * 24 * 60 * 60 - 1)
    timestamp = start + timedelta(seconds=random_seconds)
    return timestamp.strftime(formatter)

def generate_random_time_span(rnd: random.Random, year: int, mode: int = 0) -> List[str]:
    formatter = "%Y-%m-%d %H:%M:%S"
    start = datetime(year, 1, 1)
    start_seconds = rnd.randint(0, 365 * 24 * 60 * 60)
    timestamp1 = start + timedelta(seconds=start_seconds)

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

def convert_to_timestamptz(match):
    ts = match.group(1)
    return f"timestamptz '[{ts}]'"

def convert_between_format(match):
    """
    Regex replacement function to convert BETWEEN format.
    """
    timestamp1 = match.group(1).strip()
    timestamp2 = match.group(2).strip()
    
    # Validate timestamps (optional)
    try:
        datetime.strptime(timestamp1, '%Y-%m-%d %H:%M:%S')
        datetime.strptime(timestamp2, '%Y-%m-%d %H:%M:%S')
    except ValueError:
        # Return original if invalid format
        return match.group(0)
    
    return f"BETWEEN '{timestamp1}' AND '{timestamp2}'"

def process_file(input_file, output_file):
    """
    Process a file to convert all BETWEEN clauses to proper SQL format.
    
    Args:
        input_file: Path to input file
        output_file: Path to output file
    """
    # Regex pattern to match BETWEEN [timestamp, timestamp]
    pattern = re.compile(
        r"BETWEEN\s*\[\s*'([^']+)'\s*,\s*'([^']+)'\s*\]",
        re.IGNORECASE
    )
    
    with open(input_file, 'r') as infile, open(output_file, 'w') as outfile:
        for line in infile:
            # Replace all occurrences in each line
            converted_line = pattern.sub(convert_between_format, line)
            outfile.write(converted_line)

def prepare_query_tasks(config, rnd, platform="mobilityDB") -> List[QueryTask]:
    if platform == "mobilityDB":
        all_queries = []

        for query_config in config['queryConfigs']:
            if query_config['use']:
                for _ in range(query_config['repetition']):
                    sql, values = return_param_values(
                        query_config['sql'],
                        query_config['parameters'],
                        rnd,
                    )
                    pattern = r"\['([\d\-: ]+)', '([\d\-: ]+)'\]"
                    pattern_single = r"'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})'"
                    sql = re.sub(pattern, convert_to_tstzspan, sql)
                    sql = re.sub(pattern_single, convert_to_timestamptz, sql)
                    all_queries.append(QueryTask(
                        name=query_config['name'],
                        type=query_config['type'],
                        sql=LiteralString(sql),  # 👈 Ensures clean multiline SQL
                        params=values
                    ))

        if config['benchmark']['mixed']:
            random.shuffle(all_queries)

        return all_queries
    elif platform == "spatialSQL":
        all_queries = []

        for query_config in config['queryConfigs']:
            if query_config['use']:
                for _ in range(query_config['repetition']):
                    sql, values = return_param_values(
                        query_config['sql'],
                        query_config['parameters'],
                        rnd,
                    )
                    pattern = r"\['([\d\-: ]+)', '([\d\-: ]+)'\]"
                    pattern_single = r"'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})'"
                    all_queries.append(QueryTask(
                        name=query_config['name'],
                        type=query_config['type'],
                        sql=LiteralString(sql),  # 👈 Ensures clean multiline SQL
                        params=values
                    ))

        if config['benchmark']['mixed']:
            random.shuffle(all_queries)

        return all_queries
#main function
if __name__ == "__main__":
    mobilityDB_config = load_config("../config/mobilityDBBenchConf.yaml")
    # spatialSQL_config = load_config("../config/spatialSQLBenchConf.yaml")
    if not mobilityDB_config: #and not spatialSQL_config:
        exit(1)
    print("Loaded config successfully.")
    thread_count = mobilityDB_config['benchmark']['threads']
    nodes = mobilityDB_config['benchmark']['nodes']
    sut = mobilityDB_config['benchmark']['sut']
    main_seed = mobilityDB_config['benchmark']['random_seed']
    mixed = mobilityDB_config['benchmark']['mixed']
    distributed = len(nodes) > 1
    log_responses = mobilityDB_config['benchmark']['test']
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

    mobilityDB_queries = prepare_query_tasks(mobilityDB_config, main_random, "mobilityDB")
    random.shuffle(mobilityDB_queries)
    mobilityDB_data = [query.__dict__ for query in mobilityDB_queries]
    with open("../queries/mobilitydb_queries.yaml", "w") as file:
        yaml.dump(mobilityDB_data, file, sort_keys=False, allow_unicode=True)


    # spatialSQL_queries = prepare_query_tasks(spatialSQL_config, main_random, "spatialSQL")
    # random.shuffle(spatialSQL_queries)
    # spatialSQL_data = [query.__dict__ for query in spatialSQL_queries]
    # with open("../queries/spatialSQL_queries_unprocess.yaml", "w") as file:
    #     yaml.dump(spatialSQL_data, file, sort_keys=False, allow_unicode=True)
    # process_file("../queries/spatialSQL_queries_unprocess.yaml", "../queries/spatialSQL_queries.yaml")
        #remove all brackets [] from the file
    # with open("../queries/queries.yaml", "r") as file:
    #     content = file.read()
    #     content = content.replace('[', '').replace(']', '')
    # with open("../queries/queries.yaml", "w") as file:
    #     file.write(content)
    # print("Queries written to ../queries/queries.yaml")
    # #return all_queries
    #rep