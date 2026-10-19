"""Where each scenario's data is, for the query parameter generators (2026-10-19). Read-only; run on the
PostGIS coordinator:
    python3 sample_data_extent.py OUT_DIR
Writes into OUT_DIR, per scenario:
  <scenario>_sample.csv          lon,lat,timestamp of 4,000 points drawn at random from the point table
                                 (timestamps as the queries compare them: Europe/Berlin wall clock)
  <scenario>_pairs.csv           500 pairs of points (lon, lat, timestamp) of the same id (trip, crossing, flight, user)
  ais_harbours.csv               harbours with a crossing point within 5 km (the smallest harbour radius drawn)
  ais_islands.csv                island ids with a crossing point within 1 km (the smallest island distance drawn)
  aviation_cities.csv            cities with a flight point within 1 km (the smallest radius drawn)
  cycling_universities.csv       universities with a ride point within 500 m (the smallest distance drawn)
                                 (each place with up to 20 timestamps of such points)
The generators draw points, regions, time windows and places from these files, so that a query's parameters
lie where the data is. Copy the files into the scenarios' scripts/ folders (see README)."""
import csv
import os
import random
import subprocess
import sys

OUT = sys.argv[1]
os.makedirs(OUT, exist_ok=True)
ID = "id > 0 AND id < 1000000000000"
# point table, sampling percentage (whole pages, ~500k rows), id filter
TABLES = {"ais": ("crossing_points", 0.01), "aviation": ("flight_points", 0.02),
          "cycling": ("ride_points", 0.05), "geolife": ("geolife_points", 0.015)}
N = 4000


def psql(sql, ok_timeout=False):
    r = subprocess.run(["psql", "-h", "localhost", "-p", "5433", "-U", "postgres", "-d", "postgres", "-At", "-F", "\t",
                        "-v", "ON_ERROR_STOP=1", "-c", "SET TIME ZONE 'Europe/Berlin'", "-c", sql],
                       capture_output=True, text=True, env={"PGPASSWORD": "postgres", "PATH": "/usr/bin:/bin:/usr/local/bin"})
    if r.returncode != 0:
        if ok_timeout and "statement timeout" in r.stderr:
            return None
        sys.exit(f"{sql[:120]}: {r.stderr.strip()}")
    return [l.split("\t") for l in r.stdout.splitlines() if l and l not in ("SET",)]


rnd = random.Random(1500)
for sc, (table, pct) in TABLES.items():
    out = os.path.join(OUT, f"{sc}_sample.csv")
    if os.path.exists(out):
        continue
    rows = psql(f"SELECT ST_X(ST_Transform(geom, 4326)), ST_Y(ST_Transform(geom, 4326)), "
                f"to_char(timestamp, 'YYYY-MM-DD HH24:MI:SS') FROM {table} TABLESAMPLE SYSTEM ({pct}) "
                f"WHERE {ID} AND geom IS NOT NULL")
    rows = rnd.sample(rows, min(N, len(rows)))
    with open(out, "w", newline="") as f:
        csv.writer(f).writerows([["lon", "lat", "timestamp"]] + rows)
    print(sc, len(rows), "sample points, time", min(r[2] for r in rows), "to", max(r[2] for r in rows), flush=True)


def pairs(sc, table, pct, n=500):
    """Two random points (lon, lat, timestamp) of the same trip/crossing/flight/user, for queries over two regions."""
    out = os.path.join(OUT, f"{sc}_pairs.csv")
    if os.path.exists(out):
        return
    ids = [r[0] for r in psql(f"SELECT DISTINCT id FROM {table} TABLESAMPLE SYSTEM ({pct}) WHERE {ID}")]
    rows = []
    for i in rnd.sample(ids, min(n, len(ids))):
        pts = psql(f"SELECT ST_X(ST_Transform(geom, 4326)), ST_Y(ST_Transform(geom, 4326)), "
                   f"to_char(timestamp, 'YYYY-MM-DD HH24:MI:SS') FROM {table} WHERE id = {i} AND geom IS NOT NULL "
                   f"ORDER BY random() LIMIT 2")
        if len(pts) == 2:
            rows.append(pts[0] + pts[1])
    with open(out, "w", newline="") as f:
        csv.writer(f).writerows([["lon1", "lat1", "timestamp1", "lon2", "lat2", "timestamp2"]] + rows)
    print(sc, len(rows), "point pairs", flush=True)


for sc, (table, pct) in TABLES.items():
    pairs(sc, table, pct)


def places(name, keys, place_table, key_col, point_table, dist):
    """A place has data when a point lies within dist of it (as the benchmark queries join them). Keeps up to 20
    timestamps of such points (drawn from the first 2,000 found), for queries over a place and a time. A check
    that takes longer than 2 minutes counts as data (the place is kept, without timestamps)."""
    out = os.path.join(OUT, name)
    if os.path.exists(out):
        return
    keep = []
    for key in keys:
        lit = key if key_col == "id" else "'" + key.replace("'", "''") + "'"
        hit = psql(f"SET statement_timeout = '120s'; SELECT to_char(t, 'YYYY-MM-DD HH24:MI:SS') FROM ("
                   f"SELECT c.timestamp AS t FROM {point_table} c JOIN {place_table} p ON ST_DWithin(c.geom, p.geom, {dist}) "
                   f"WHERE p.{key_col} = {lit} AND c.id > 0 AND c.id < 1000000000000 LIMIT 2000) s "
                   f"ORDER BY random() LIMIT 20", ok_timeout=True)
        if hit is None or hit:
            keep.append([key, ";".join(r[0] for r in hit or [])])
    with open(out, "w", newline="") as f:
        csv.writer(f).writerows([["key", "timestamps"]] + keep)
    print(name, len(keep), "of", len(keys), "with data", flush=True)


places("ais_harbours.csv", [r[0] for r in psql("SELECT DISTINCT name FROM harbours WHERE name IS NOT NULL ORDER BY name")],
       "harbours", "name", "crossing_points", 5000)
places("ais_islands.csv", [r[0] for r in psql("SELECT id FROM islands ORDER BY id")], "islands", "id", "crossing_points", 1000)
places("aviation_cities.csv", [r[0] for r in psql("SELECT DISTINCT name FROM cities ORDER BY name")],
       "cities", "name", "flight_points", 1000)
places("cycling_universities.csv", [r[0] for r in psql("SELECT DISTINCT name FROM universities ORDER BY name")],
       "universities", "name", "ride_points", 500)
print("DONE")
