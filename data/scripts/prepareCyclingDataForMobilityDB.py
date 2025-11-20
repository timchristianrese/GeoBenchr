import os
import csv
import random
import numpy as np
from datetime import datetime, timezone

double_ts = 0

def generate_rider_distribution(n_riders=6000, mean=3, std=1):
    rider_traj_counts = []
    for _ in range(n_riders):
        traj_count = max(1, int(np.random.normal(loc=mean, scale=std)))
        rider_traj_counts.append(traj_count)
    return rider_traj_counts

def build_rider_ids(rider_traj_counts):
    rider_id_list = []
    for rider_id, nb_traj in enumerate(rider_traj_counts):
        rider_id_list.extend([rider_id] * nb_traj)
    random.shuffle(rider_id_list)
    return rider_id_list

def find_separator_index(lines):
    for i, line in enumerate(lines):
        if line.strip().strip('=') == '' and len(line.strip()) >= 5:
            return i
    return -1

def format_tgeogpoint(coords, times):
    global double_ts
    points = []
    seen_times = set()

    for (lon, lat), ts in zip(coords, times):
        try:
            if ts.isdigit():
                t = datetime.fromtimestamp(int(ts) / 1000, tz=timezone.utc).isoformat()
            else:
                t = datetime.fromisoformat(ts).isoformat()
        except ValueError:
            t = ts

        if t in seen_times:
            double_ts += 1
            continue 

        seen_times.add(t)
        points.append(f"Point({lon} {lat})@{t}")

    return f"[{', '.join(points)}]" if points else None

def extract_data(filepath, ride_writer, ride_id_start, rider_id_iter, incident_writer, incident_id_start):
    with open(filepath, 'r', encoding='utf-8') as f:
        lines = f.read().splitlines()


    separator_index = find_separator_index(lines)
    if separator_index == -1:
        print(f"Ignored file: {filepath} (missing separator)")
        return ride_id_start

    i_header = lines[1].split(",")
    i_data = lines[2:separator_index]
    r_header = lines[separator_index+2].split(",")
    r_data = lines[separator_index+3:]

    try:
        i_lon_idx = i_header.index("lon")
        i_lat_idx = i_header.index("lat")
        i_ts_idx = i_header.index("ts")  
        i_key_idx = i_header.index("key")      
        r_lon_idx = r_header.index("lon")
        r_lat_idx = r_header.index("lat")
        r_ts_idx = r_header.index("timeStamp")
    except ValueError as e:
        print(f"Missing expected columns in {filepath}: {e}")
        return ride_id_start
    
    for row in i_data:
        if not row.strip():
            continue
        parts = row.split(",")

        parts += [""] * (len(i_header) - len(parts))

        try:
            lon = parts[i_lon_idx].strip()
            lat = parts[i_lat_idx].strip()
            ts = parts[i_ts_idx].strip()
        except ValueError:
            continue

        if lat and lon and ts:
            del parts[i_key_idx]
            row_data = parts + [ride_id_start]
            incident_writer.writerow([incident_id_start] + row_data)
            incident_id_start += 1


    coords = []
    times = []

    for row in r_data:
        parts = row.split(",")
        lon = parts[r_lon_idx].strip()
        lat = parts[r_lat_idx].strip()
        ts = parts[r_ts_idx].strip()

        if lon and lat and ts:
            coords.append((lon, lat))
            times.append(ts)

    if coords and times:
        tgeog = format_tgeogpoint(coords, times)
        if tgeog:
            try:
                rider_id = next(rider_id_iter)
            except StopIteration:
                rider_id = random.randint(0, 6000)
            ride_writer.writerow([ride_id_start, rider_id, tgeog])
            ride_id_start += 1

    return ride_id_start, incident_id_start

def process_all_files(source_root, output_root):
    os.makedirs(output_root, exist_ok=True)
    ride_id = 1
    incident_id = 1

    rider_traj_counts = generate_rider_distribution(n_riders=2000, mean=3, std=1)
    rider_id_pool = build_rider_ids(rider_traj_counts)
    rider_id_iter = iter(rider_id_pool)


    for folder in sorted(os.listdir(source_root)):
        if folder.startswith("Berlin_20") and os.path.isdir(os.path.join(source_root, folder)):
            source_folder_path = os.path.join(source_root, folder)
            target_folder_path = os.path.join(output_root, folder)

            os.makedirs(target_folder_path, exist_ok=True)

            print(f"Folder : {folder} → {target_folder_path}")

            rides_path = os.path.join(target_folder_path, "rides.csv")
            incidents_path = os.path.join(target_folder_path, "incidents.csv")

            with open(rides_path, 'w', newline='', encoding='utf-8') as ride_file, open(incidents_path, 'w', newline='', encoding='utf-8') as incident_file:

                ride_writer = csv.writer(ride_file)
                incident_writer = csv.writer(incident_file)

                ride_writer.writerow(["ride_id", "rider_id","traj"])
                incident_writer.writerow(["incident_id", "lat", "lon", "ts", "bike", "childCheckBox",
                                          "trailerCheckBox", "pLoc", "incident", "i1", "i2", "i3", "i4", "i5",
                                          "i6", "i7", "i8", "i9", "scary", "desc", "i10", "ride_id"])

                for file in os.listdir(source_folder_path):
                    if file.startswith("VM"):
                        filepath = os.path.join(source_folder_path, file)
                        try:
                            ride_id, incident_id = extract_data(filepath, ride_writer, ride_id, rider_id_iter, incident_writer, incident_id)
                        except Exception as e:
                            print(f"Error in {filepath} : {e}")


if __name__ == "__main__":
    data_root = "./dataset-master/"
    output_dir = "dataMobilityDB"
    process_all_files(data_root, output_dir)
    print(f"Processing complete. Files rides.csv and incidents.csv have been generated without {double_ts} double timestamps.")
