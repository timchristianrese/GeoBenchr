import os
import glob
import folium
import random
from datetime import datetime, timedelta

def read_trajectory(file_path):
    coords = []
    timestamps = []
    with open(file_path, 'r', encoding='utf-8') as f:
        next(f)
        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = line.split(',')
            if len(parts) < 3:
                continue
            try:
                lat = float(parts[0])
                lon = float(parts[1])
                ts = int(parts[2])  # timestamp in ms
                coords.append((lat, lon))
                timestamps.append(ts)
            except ValueError:
                continue
    return coords, timestamps

def random_color():
    return "#{:06x}".format(random.randint(0, 0xFFFFFF))

def random_start_date_2024():
    """Return a random datetime in 2024"""
    start = datetime(2024, 1, 1)
    end = datetime(2025, 1, 1)
    delta = end - start
    random_seconds = random.randint(0, int(delta.total_seconds()))
    return start + timedelta(seconds=random_seconds)

def plot_trajectories_on_map(traj_folder, output_file):
    traj_files = glob.glob(os.path.join(traj_folder, "VM2_-*"))
    if not traj_files:
        print(f"No trajectory files found in {traj_folder}")
        return

    m = folium.Map(location=[52.52, 13.405], zoom_start=13)

    for file_path in traj_files:
        coords, timestamps = read_trajectory(file_path)
        if len(coords) < 2 or len(timestamps) != len(coords):
            continue

        start_date = random_start_date_2024()

        min_ts = min(timestamps)

        points = []
        for (lat, lon), ts in zip(coords, timestamps):
            # Calcul date+heure: start_date + (ts - min_ts) in ms
            delta_ms = ts - min_ts
            point_date = start_date + timedelta(milliseconds=delta_ms)
            popup = point_date.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            points.append(folium.CircleMarker(
                location=(lat, lon),
                radius=3,
                color="blue",
                fill=True,
                fill_opacity=0.7,
                popup=popup
            ))

        color = random_color()
        folium.PolyLine(coords, color=color, weight=3, opacity=0.7,
                        tooltip=f"{os.path.basename(file_path)} - début {start_date.strftime('%Y-%m-%d %H:%M:%S')}").add_to(m)

        folium.Marker(coords[0], tooltip=f"Départ - {os.path.basename(file_path)}", icon=folium.Icon(color="green")).add_to(m)
        folium.Marker(coords[-1], tooltip=f"Arrivée - {os.path.basename(file_path)}", icon=folium.Icon(color="red")).add_to(m)

        for p in points:
            p.add_to(m)

    m.save(output_file)
    print(f"Saved map in : {output_file}")

if __name__ == "__main__":
    traj_folder = "./output/all"
    output_map_file = "./output/generated_trajectories_map.html"
    os.makedirs(os.path.dirname(output_map_file), exist_ok=True)

    plot_trajectories_on_map(traj_folder, output_map_file)
