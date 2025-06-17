import os
import random
import itertools
import pandas as pd
import folium
from pathlib import Path

NUM_FILES = 10
INPUT_DIR = Path("generated")
OUTPUT_HTML = INPUT_DIR / "generated_map.html"

files = sorted([f for f in INPUT_DIR.glob("*.csv")])
if not files:
    print(f"no csv in {INPUT_DIR.resolve()}")
    exit(1)

files = random.sample(files, min(NUM_FILES, len(files)))
print(f"{len(files)} trajectories loaded from {INPUT_DIR}")

first_df = pd.read_csv(files[0])
if first_df.empty:
    print("first file empty, cannot center map")
    exit(1)

m = folium.Map(
    location=[first_df["lat"].iloc[0], first_df["lon"].iloc[0]],
    zoom_start=6,
    tiles="cartodbpositron"
)

colors = itertools.cycle([
    'red', 'blue', 'green', 'purple', 'orange',
    'black', 'pink', 'darkgreen', 'cadetblue', 'darkred'
])

for fpath in files:
    df = pd.read_csv(fpath)
    if df.empty:
        print(f"Ignored empty file: {fpath.name}")
        continue

    color = next(colors)
    coords = list(zip(df["lat"], df["lon"]))

    folium.PolyLine(coords, color=color, weight=2.5, opacity=0.7).add_to(m)

    for lat, lon in coords:
        folium.CircleMarker(
            location=[lat, lon],
            radius=2,
            color=color,
            fill=True,
            fill_opacity=0.6
        ).add_to(m)

m.save(OUTPUT_HTML)
print(f"Map : {OUTPUT_HTML.resolve()}")