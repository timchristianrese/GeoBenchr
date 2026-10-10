import os
import pandas as pd
import rasterio
from shapely.wkt import loads as wkt_loads
from datetime import datetime
from tqdm import tqdm
from collections import defaultdict

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_PATH = os.path.join(BASE_DIR, 'raw_data/csvdata/AIS/point_unipi_ais_dynamic_apr2019.csv')
RASTER_PATH = os.path.join(BASE_DIR, 'raw_data/waterGroundData/gebco.tif')
OUTPUT_DIR = os.path.join(BASE_DIR, 'output_ais_txt')
os.makedirs(OUTPUT_DIR, exist_ok=True)

print("Loading GEBCO raster...") # used for depth
depth_raster = rasterio.open(RASTER_PATH)
depth_data = depth_raster.read(1)

depth_cache = {}

def get_depth(lon, lat):
    key = (round(lon, 5), round(lat, 5))
    if key in depth_cache:
        return depth_cache[key]
    try:
        row, col = depth_raster.index(lon, lat)
        if 0 <= row < depth_data.shape[0] and 0 <= col < depth_data.shape[1]:
            depth = float(depth_data[row, col])
        else:
            depth = None
    except:
        depth = None
    depth_cache[key] = depth
    return depth

# Parse WKT and timestamp
def parse_point(wkt_str):
    try:
        point = wkt_loads(wkt_str)
        return point.y, point.x  # lat, lon
    except:
        return None, None

def parse_timestamp(ts_str):
    try:
        dt = datetime.strptime(ts_str, '%Y-%m-%d %H:%M:%S')
        return int(dt.timestamp() * 1000)
    except:
        return None

print("Processing AIS CSV data in chunks...")
chunksize = 100000
vessel_data = defaultdict(list)

total_rows = 0
for chunk in pd.read_csv(CSV_PATH, usecols=['timestamp', 'vessel_id', 'wkt', 'speed', 'course'], chunksize=chunksize):
    chunk = chunk.dropna(subset=['timestamp', 'vessel_id', 'wkt'])
    chunk[['lat', 'lon']] = chunk['wkt'].apply(lambda w: pd.Series(parse_point(w)))
    chunk['timestamp_ms'] = chunk['timestamp'].apply(parse_timestamp)
    chunk = chunk.dropna(subset=['lat', 'lon', 'timestamp_ms'])

    for _, row in chunk.iterrows():
        vessel_id = row['vessel_id']
        vessel_data[vessel_id].append(row)

    total_rows += len(chunk)
    print(f"en cours {total_rows:,} colonnes")

print("Writing vessel trajectory files...")
GAP_THRESHOLD_MS = 3600 * 1000

for i, (vessel_id, rows) in enumerate(tqdm(vessel_data.items())):
    rows.sort(key=lambda r: r['timestamp_ms'])
    
    traj_idx = 0
    current_traj = []
    last_ts = None

    for row in rows:
        ts = row['timestamp_ms']
        if last_ts is None or ts - last_ts <= GAP_THRESHOLD_MS:
            current_traj.append(row)
        else:
            if len(current_traj) > 1:
                output_file = os.path.join(OUTPUT_DIR, f"vessel_{vessel_id}_trip_{traj_idx}.txt")
                with open(output_file, 'w', encoding='utf-8') as f:
                    f.write("lat,lon,timestamp_ms,depth,speed,course\n")
                    for r in current_traj:
                        lat, lon = r['lat'], r['lon']
                        depth = get_depth(lon, lat)
                        speed = r.get('speed', '')
                        course = r.get('course', '')
                        if depth is not None:
                            f.write(f"{lat},{lon},{r['timestamp_ms']},{depth},{speed},{course}\n")
                traj_idx += 1
            current_traj = [row]
        last_ts = ts

    if len(current_traj) > 1:
        output_file = os.path.join(OUTPUT_DIR, f"vessel_{vessel_id}_trip_{traj_idx}.txt")
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write("lat,lon,timestamp_ms,depth,speed,course\n")
            for r in current_traj:
                lat, lon = r['lat'], r['lon']
                depth = get_depth(lon, lat)
                speed = r.get('speed', '')
                course = r.get('course', '')
                if depth is not None:
                    f.write(f"{lat},{lon},{r['timestamp_ms']},{depth},{speed},{course}\n")

    if i % 100 == 0:
        print(f"Saved {i} vessel files..")

print("Processing complete.")