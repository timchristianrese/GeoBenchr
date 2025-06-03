import os
import pandas as pd
import numpy as np
from geopy.distance import geodesic
import json

def extract_latlon_timestamp_block(path):
    with open(path, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    data_block = []
    header = []
    recording = False

    for line in lines:
        line = line.strip()

        if not line or line.startswith('='):
            recording = False
            continue
        if 'lat' in line and 'lon' in line and 'timeStamp' in line:
            header = line.split(',')
            recording = True
            continue

        if recording:
            data_block.append(line.split(','))
    if not header or not data_block:
        return None

    try:
        df = pd.DataFrame(data_block, columns=header)
        df = df[['lat', 'lon', 'timeStamp']].dropna()
        df = df[(df['lat'] != '') & (df['lon'] != '') & (df['timeStamp'] != '')]

        df['lat'] = df['lat'].astype(float)
        df['lon'] = df['lon'].astype(float)
        df['timeStamp'] = pd.to_datetime(df['timeStamp'].astype(np.int64), unit='ms')
        df = df.sort_values(by='timeStamp').reset_index(drop=True)
        return df
    except Exception as e:
        print(f"Error when converting {path} : {e}")
        return None

def compute_speed_features(df):
    distances, deltas, speeds = [], [], []

    for i in range(1, len(df)):
        p1 = (df.loc[i-1, 'lat'], df.loc[i-1, 'lon'])
        p2 = (df.loc[i, 'lat'], df.loc[i, 'lon'])

        d = geodesic(p1, p2).meters
        dt = (df.loc[i, 'timeStamp'] - df.loc[i-1, 'timeStamp']).total_seconds()
        v = d / dt if dt > 0 else 0

        distances.append(d)
        deltas.append(dt)
        speeds.append(v)

    df['distance_m'] = [np.nan] + distances
    df['delta_t_s'] = [np.nan] + deltas
    df['speed_mps'] = [np.nan] + speeds
    return df

def analyze_all_trajectories():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    input_dir = os.path.join(base_dir, 'all')
    output_dir = os.path.join(base_dir, 'output')
    os.makedirs(output_dir, exist_ok=True)

    stats = []
    traj_files = [f for f in os.listdir(input_dir) if f.startswith('VM2_')]

    if not traj_files:
        print("no file VM2_* in ./all")
        return

    for file in traj_files:
        path = os.path.join(input_dir, file)
        traj = extract_latlon_timestamp_block(path)
        if traj is None or len(traj) < 2:
            print(f"file ignored len  : {file}")
            continue

        traj = compute_speed_features(traj).dropna()

        stats.append({
            "file": file,
            "duration_s": (traj['timeStamp'].iloc[-1] - traj['timeStamp'].iloc[0]).total_seconds(),
            "distance_m": traj['distance_m'].sum(),
            "speed_min": traj['speed_mps'].min(),
            "speed_max": traj['speed_mps'].max(),
            "speed_mean": traj['speed_mps'].mean()
        })

    df_stats = pd.DataFrame(stats)
    df_stats.to_csv(os.path.join(output_dir, "trajectory_speeds.csv"), index=False)

    summary = {
        "global_speed_min": df_stats["speed_min"].min(),
        "global_speed_max": df_stats["speed_max"].max(),
        "global_speed_mean": df_stats["speed_mean"].mean(),
        "duration_mean": df_stats["duration_s"].mean(),
        "distance_mean": df_stats["distance_m"].mean(),
        "n_trajectories": len(df_stats)
    }

    with open(os.path.join(output_dir, "speed_summary.json"), "w") as f:
        json.dump(summary, f, indent=2)

    print("✅ Extraction terminée.")
    print(f"  → {len(df_stats)} trajectoires analysées")
    print(f"  → Résumé disponible dans: {output_dir}/speed_summary.json")

if __name__ == "__main__":
    analyze_all_trajectories()
