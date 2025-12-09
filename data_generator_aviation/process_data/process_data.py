#!/usr/bin/env python3
import os
import sys
import math
from pathlib import Path
from datetime import datetime
from collections import defaultdict

import pandas as pd
from shapely.wkt import loads as wkt_loads
from tqdm import tqdm

CHUNKSIZE  = 200_000
OUTPUT_DIR = Path("flights_txt")
OUTPUT_DIR.mkdir(exist_ok=True)

print("Received arguments", sys.argv[1:], flush=True)

def parse_point(w):
    try:
        p = wkt_loads(w)
        return p.y, p.x
    except Exception:
        return None, None

def ts_sec(ts):
    try:
        return int(datetime.strptime(ts, "%Y-%m-%d %H:%M:%S").timestamp())
    except Exception:
        return None

def haversine(lat1, lon1, lat2, lon2):
    R = 6371.0
    dlat, dlon = map(math.radians, [lat2 - lat1, lon2 - lon1])
    a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    dist_nm = R * c * 0.539957

    y = math.sin(dlon) * math.cos(math.radians(lat2))
    x = (math.cos(math.radians(lat1)) * math.sin(math.radians(lat2))
         - math.sin(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.cos(dlon))
    bearing = (math.degrees(math.atan2(y, x)) + 360) % 360

    return dist_nm, bearing

def write_flight(track, rows):
    rows.sort(key=lambda r: r.ts_s)
    fname = OUTPUT_DIR / f"flight_{track}.txt"

    with open(fname, "w", encoding="utf-8") as f:
        f.write("time_iso,lat,lon,delta_t_s,alt_ft,gs_kt,heading_deg\n")
        prev = None
        for r in rows:
            dt = 0 if prev is None else r.ts_s - prev.ts_s
            if prev is None:
                gs = hdg = ""
            else:
                d_nm, hdg = haversine(prev.lat, prev.lon, r.lat, r.lon)
                gs = round(d_nm / (dt / 3600.0), 1) if dt > 0 else ""
                hdg = round(hdg, 1)
            f.write(f"{r.ts},{r.lat:.6f},{r.lon:.6f},{dt},{r.alt_ft:.1f},{gs},{hdg}\n")
            prev = r

if len(sys.argv) < 2:
    sys.exit("Usage: python process_data.py <csv1> [csv2] ...")

for csvfile in sys.argv[1:]:
    print("\n=== Processing", csvfile, "===")
    flights = defaultdict(list)
    total_points = 0

    # --- read CSV in chunks with progress bar ---
    reader = pd.read_csv(
        csvfile,
        sep=";",
        header=None,
        names=["track_id", "ac_type", "origin", "dest", "point", "ts", "alt_ft"],
        dtype={"track_id": str, "point": str, "ts": str, "alt_ft": float},
        chunksize=CHUNKSIZE
    )

    for chunk_idx, chunk in enumerate(tqdm(reader, desc="Reading", unit="chunk")):
        # parse point and timestamp
        chunk[["lat", "lon"]] = chunk["point"].apply(lambda w: pd.Series(parse_point(w)))
        chunk["ts_s"] = chunk["ts"].apply(ts_sec)
        chunk.dropna(subset=["lat", "lon", "ts_s"], inplace=True)

        # group by flight ID
        for _, r in chunk.iterrows():
            flights[r.track_id].append(r)

        total_points += len(chunk)
        tqdm.write(f"  chunk {chunk_idx:>4} contains {len(chunk):,} points, total {total_points:,}")

    # --- write processed flights ---
    for track, rows in tqdm(flights.items(), desc="Writing flights", unit="flight"):
        write_flight(track, rows)

    print(f"Done with {csvfile}, {len(flights):,} flights, {total_points:,} points\n")