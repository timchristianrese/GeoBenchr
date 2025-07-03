import os
import glob
import random
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import folium

#TRAJ_FOLDER = Path("flights_txt_for_training")
TRAJ_FOLDER = Path("generated")
OUTPUT_HTML = TRAJ_FOLDER / "flights_map.html"
MAX_TRAJ    = 500 # nb flight displayed


def read_trajectory(file_path: Path):
    """Return coordinates list and timestamps list from a flight file"""
    try:
        df = pd.read_csv(file_path)
    except Exception:
        return [], []

    if {"lat", "lon", "delta_t_s"}.issubset(df.columns):
        # timestamps are relative seconds, convert to ms
        ts = df["delta_t_s"].fillna(0).cumsum() * 1000.0
    elif {"time_iso"}.issubset(df.columns):
        ts = pd.to_datetime(df["time_iso"]).astype(int) // 1_000_000
    else:
        # cannot understand format
        return [], []

    coords = list(zip(df["lat"], df["lon"]))
    return coords, ts.tolist()


def random_color():
    return "#{:06x}".format(random.randint(0, 0xFFFFFF))


def random_start_date_2024():
    start = datetime(2024, 1, 1)
    end   = datetime(2025, 1, 1)
    delta = end - start
    return start + timedelta(seconds=random.randint(0, int(delta.total_seconds())))


def plot_trajectories(folder: Path, out_html: Path, max_traj: int):
    files = sorted(list(folder.glob("*.txt")) + list(folder.glob("*.csv")))
    if not files:
        print(f"No trajectory files in {folder}")
        return

    if max_traj is not None and len(files) > max_traj:
        files = random.sample(files, max_traj)

    print(f"{len(files)} trajectories loaded from {folder}")

    # center map on first file
    first_df = pd.read_csv(files[0])
    m = folium.Map(
        location=[first_df["lat"].iloc[0], first_df["lon"].iloc[0]],
        zoom_start=6,
        tiles="cartodbpositron"
    )

    for fpath in files:
        coords, timestamps = read_trajectory(fpath)
        if len(coords) < 2 or len(coords) != len(timestamps):
            print(f"Skipped {fpath.name} (bad format)")
            continue

        color = random_color()
        start_date = random_start_date_2024()
        min_ts = min(timestamps)

        # draw line
        folium.PolyLine(
            coords,
            color=color,
            weight=2.5,
            opacity=0.7,
            tooltip=f"{fpath.name}"
        ).add_to(m)

        # draw points
        for (lat, lon), ts in zip(coords, timestamps):
            point_time = start_date + timedelta(milliseconds=int(ts - min_ts))
            folium.CircleMarker(
                location=(lat, lon),
                radius=2,
                color=color,
                fill=True,
                fill_opacity=0.6,
                popup=point_time.strftime("%Y-%m-%d %H:%M:%S")
            ).add_to(m)

    m.save(out_html)
    print("Map saved to", out_html.resolve())


if __name__ == "__main__":
    os.makedirs(TRAJ_FOLDER, exist_ok=True)
    plot_trajectories(TRAJ_FOLDER, OUTPUT_HTML, MAX_TRAJ)