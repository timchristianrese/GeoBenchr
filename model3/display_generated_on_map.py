import os
import pandas as pd
import folium

MAX_FILES = 1  # how many trajectories are displayed

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TRAJECTORY_DIR = os.path.join(BASE_DIR, 'output')
OUTPUT_MAP = os.path.join(BASE_DIR, 'output', 'generated_trajectories_map.html')

files = [f for f in os.listdir(TRAJECTORY_DIR) if f.startswith("generated_trajectory_") and f.endswith(".txt")]
files = sorted(files)[:MAX_FILES]

if not files:
    print("No generated trajectory files found.")
    exit()

first_df = pd.read_csv(os.path.join(TRAJECTORY_DIR, files[0]))
m = folium.Map(location=[first_df['lat'].iloc[0], first_df['lon'].iloc[0]], zoom_start=10)

#Add points from each file
for file in files:
    df = pd.read_csv(os.path.join(TRAJECTORY_DIR, file))
    for _, row in df.iterrows():
        folium.CircleMarker(
            location=[row['lat'], row['lon']],
            radius=2,
            color='blue',
            fill=True,
            fill_opacity=0.7
        ).add_to(m)

m.save(OUTPUT_MAP)
print(f"Map with {MAX_FILES} files saved to {OUTPUT_MAP}")