import random
from datetime import datetime, timedelta
from typing import List, Any, Optional
import csv
import os
import yaml
from dataclasses import dataclass
import re

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

# --- Config loading ---
def load_config(path="../config/combinedBenchConf.yaml"):
    try:
        with open(path) as f:
            return yaml.safe_load(f)
    except Exception as e:
        print(f"Failed to load config: {e}")
        return None

# --- Parameter substitution ---
def return_param_values(sql1, sql2, sql3, sql4, params, rnd) -> List[Any]:
    parsedSQL1, parsedSQL2, parsedSQL3, parsedSQL4 = sql1, sql2, sql3, sql4
    values = []

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
            "port": lambda: get_random_place("harbours", rnd),
            "island": lambda: get_random_place("islands-wkt", rnd),
            "region": lambda: get_random_place("regions-wkt", rnd, ";"),
            "point": lambda: get_random_point(rnd, [[-10, 60], [30, 35]]),
            "radius": lambda: (rnd.uniform(2.0, 10.0) * 10) / (1000 * 6378),
            "distance": lambda: rnd.randint(1, 10)
        }.get(param, lambda: "")()

        if replacement is not None:
            if isinstance(replacement, str):
                quoted = f"'{replacement}'"
            elif isinstance(replacement, list):
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

# --- Helpers ---
def get_random_point(rnd, rect):
    ul_lon, ul_lat = rect[0]
    br_lon, br_lat = rect[1]
    lon = ul_lon + rnd.random() * (br_lon - ul_lon)
    lat = br_lat + rnd.random() * (ul_lat - br_lat)
    return [lon, lat]

def get_random_day(rnd, year):
    start_date = datetime(year, 1, 1)
    end_date = datetime(year, 12, 31)
    delta = end_date - start_date
    return (start_date + timedelta(days=rnd.randint(0, delta.days))).strftime("%Y-%m-%d")

def get_random_hour(rnd): return rnd.randint(0, 23)

def generate_random_timestamp(rnd):
    year = 2019
    start = datetime(year, 1, 1)
    ts = start + timedelta(seconds=rnd.randint(0, 365 * 24 * 60 * 60 - 1))
    return ts.strftime("%Y-%m-%d %H:%M:%S")

def generate_random_time_span(rnd, year, mode=0):
    start = datetime(year, 1, 1)
    ts1 = start + timedelta(seconds=rnd.randint(0, 365 * 24 * 60 * 60))
    mode_ranges = {1: (0, 2*24*60*60), 2: (2*24*60*60, 15*24*60*60), 3: (15*24*60*60, 365*24*60*60)}
    seconds = rnd.randint(*mode_ranges.get(mode, (0, 365*24*60*60)))
    ts2 = ts1 + timedelta(seconds=rnd.choice([-1, 1]) * seconds)
    ts2 = max(datetime(year, 1, 1), min(datetime(year, 12, 31, 23, 59, 59), ts2))
    return sorted([ts1, ts2], key=lambda t: t) if ts1 < ts2 else [ts2, ts1]

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
def convert_to_timestamptz(m): return f"timestamptz '[{m.group(1)}]'"
def fix_between_clause(sql): return re.sub(r"BETWEEN\s*\[\s*'([^']+)'\s*,\s*'([^']+)'\s*\]", r"BETWEEN TIMESTAMP('\1') AND TIMESTAMP('\2')", sql)
def adjust_timestamp_format(ts): return re.sub(r"\['([\d\-: ]+)', '([\d\-: ]+)'\]", r"'[\1, \2]'", ts)

def convert_between_format(m): return f"BETWEEN '{m.group(1)}' AND '{m.group(2)}'"
def process_file(input_file, output_file):
    pattern = re.compile(r"BETWEEN\s*\[\s*'([^']+)'\s*,\s*'([^']+)'\s*\]", re.IGNORECASE)
    with open(input_file, 'r') as infile, open(output_file, 'w') as outfile:
        for line in infile:
            outfile.write(pattern.sub(convert_between_format, line))

# --- Query preparation ---
def prepare_query_tasks(config, rnd):
    mob, pg, sed, st = [], [], [], []
    for q in config['queryConfigs']:
        if q['use']:
            for _ in range(q['repetition']):
                sql1, sql2, sql3, sql4, values = return_param_values(q['mobilitydb'], q['postgis'], q['sedona'], q['spacetime'], q['parameters'], rnd)
                sql1 = re.sub(r"\['([\d\-: ]+)', '([\d\-: ]+)'\]", convert_to_tstzspan, sql1)
                sql1 = re.sub(r"'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})'", convert_to_timestamptz, sql1)
                mob.append(QueryTask(q['name'], q['type'], LiteralString(sql1), values))
                pg.append(QueryTask(q['name'], q['type'], LiteralString(sql2), values))
                sed.append(QueryTask(q['name'], q['type'], LiteralString(fix_between_clause(sql3)), values))
                st.append(QueryTask(q['name'], q['type'], LiteralString(sql4), values))
    if config['benchmark']['mixed']:
        random.shuffle(sed)
    return mob, pg, sed, st

# --- Main ---
if __name__ == "__main__":
    config = load_config("../config/combinedBenchConf.yaml")
    if not config: exit(1)

    main_random = random.Random(config['benchmark']['random_seed'])
    mob, pg, sed, st = prepare_query_tasks(config, main_random)

    indices = list(range(len(mob)))
    main_random.shuffle(indices)
    mob, pg, sed, st = [[arr[i] for i in indices] for arr in (mob, pg, sed, st)]

    # Write outputs
    with open("../queries/mobilitydb_queries.yaml", "w") as f: yaml.dump([q.__dict__ for q in mob], f, sort_keys=False, allow_unicode=True)
    with open("../queries/postgisSQL_queries_unprocess.yaml", "w") as f: yaml.dump([q.__dict__ for q in pg], f, sort_keys=False, allow_unicode=True)
    process_file("../queries/postgisSQL_queries_unprocess.yaml", "../queries/postgisSQL_queries.yaml")
    with open("../queries/postgisSQL_queries.yaml") as f: 
        content = f.read().replace("crossing_points", "tsdb_crossing_points")
    with open("../queries/tsdb_queries.yaml", "w") as f: f.write(content)
    with open("../queries/sedonaSQL_queries.yaml", "w") as f: yaml.dump([q.__dict__ for q in sed], f, sort_keys=False, allow_unicode=True)
    with open("../queries/spaceTimeSQL_queries.yaml", "w") as f: yaml.dump([q.__dict__ for q in st], f, sort_keys=False, allow_unicode=True)
    with open("../queries/spaceTimeSQL_queries.yaml") as f: content = adjust_timestamp_format(f.read())
    with open("../queries/spaceTimeSQL_queries.yaml", "w") as f: f.write(content)
