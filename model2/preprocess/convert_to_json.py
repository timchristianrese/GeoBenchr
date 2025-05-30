import os
import json

base_dir = os.path.dirname(os.path.abspath(__file__))
input_folder = os.path.join(base_dir, "../..", "data_files", "Berlin", "Rides")
output_folder = os.path.join(base_dir, "..", "data", "raw_trips", "human")
os.makedirs(output_folder, exist_ok=True)

def extract_gps_points(filepath):
    """
    Read a raw text file and extract GPS points (longitude, latitude).

    The file starts with a separator line '========================='.
    Only lines after this separator are parsed.
    """
    coords = []
    with open(filepath, "r") as f:
        start = False
        for line in f:
            line = line.strip()
            if line == "=========================":
                start = True
                continue
            if not start:
                continue
            if line and not line.startswith(","):
                parts = line.split(",")
                try:
                    lat = float(parts[0])
                    lon = float(parts[1])
                    coords.append([lon, lat])  # Format (lon, lat)
                except (ValueError, IndexError):
                    continue
    return coords

# Loop through all files in the input folder
for filename in os.listdir(input_folder):
    if filename.startswith("VM2_-"): # Only process files with this prefix
        filepath = os.path.join(input_folder, filename)
        coords = extract_gps_points(filepath)
        if len(coords) > 5:  # Only save trips with more than 5 points
            output_path = os.path.join(output_folder, filename + ".json")
            with open(output_path, "w") as out:
                json.dump(coords, out)
            print(f"[OK] {filename} → {len(coords)} points")
