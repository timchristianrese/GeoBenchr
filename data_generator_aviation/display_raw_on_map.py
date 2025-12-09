import random
import itertools
from pathlib import Path

import pandas as pd
import folium

# ─── CONFIGURATION ───
NUM_FILES   = 100                        # -1 means "use all files"
INPUT_DIR   = Path("flights_txt")       # Directory containing raw .txt flight files
OUTPUT_HTML = INPUT_DIR / "raw_map.html"
DRAW_POINTS = True                      # Whether to draw circles for each GPS point
# ─────────────────────

files = sorted(INPUT_DIR.glob("*.txt"))
if not files:
    print(f"No .txt files found in {INPUT_DIR.resolve()}")
    exit(1)

if NUM_FILES != -1:
    files = random.sample(files, min(NUM_FILES, len(files)))

print(f"{len(files)} trajectory files loaded from {INPUT_DIR}")

first_df = pd.read_csv(files[0])
if first_df.empty:
    print("First file is empty, cannot center map")
    exit(1)

m = folium.Map(
    location=[first_df["lat"].iloc[0], first_df["lon"].iloc[0]],
    zoom_start=6,
    tiles="cartodbpositron"
)

color_cycle = itertools.cycle([
    "red", "blue", "green", "purple", "orange",
    "black", "pink", "darkgreen", "cadetblue", "darkred",
])

for fpath in files:
    df = pd.read_csv(fpath)
    if df.empty:
        print(f"Empty file skipped: {fpath.name}")
        continue

    color = next(color_cycle)
    coords = list(zip(df["lat"], df["lon"]))

    folium.PolyLine(coords, color=color, weight=2.5, opacity=0.7).add_to(m)

    if DRAW_POINTS:
        for lat, lon in coords:
            folium.CircleMarker(
                location=[lat, lon],
                radius=2,
                color=color,
                fill=True,
                fill_opacity=0.6
            ).add_to(m)

# ─── SAVE OUTPUT ───
m.save(OUTPUT_HTML)
print(f"Map saved at {OUTPUT_HTML.resolve()}")