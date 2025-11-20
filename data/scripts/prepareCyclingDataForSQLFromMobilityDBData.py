import os
import csv
import re
import sys

csv.field_size_limit(sys.maxsize)

def parse_tgeogpoint(traj: str):

    pattern = re.compile(r'(Point\([^)]+\))@([^,\]]+)')
    points = []
    times = []
    for pt, ts in pattern.findall(traj):
        points.append(pt)
        times.append(ts)
    return points, times

def extract_data(filepath, ride_writer):
    with open(filepath, 'r', encoding='utf-8') as f:
        lines = f.read().splitlines()


    header = lines[0].split(",")
    data = lines[1:]

    try:
        ride_id_idx = header.index("ride_id")
        rider_id_idx = header.index("rider_id")
        traj_idx = header.index("traj")
    except ValueError as e:
        print(f"Missing expected columns in {filepath}: {e}")
    

    coords = []
    times = []

    for row in data:
        parts = list(csv.reader([row]))[0]
        ride_id = parts[ride_id_idx].strip()
        rider_id = parts[rider_id_idx].strip()
        traj = parts[traj_idx].strip()

        points, times = parse_tgeogpoint(traj)
        for pt, ts in zip(points, times):
            ride_writer.writerow([ride_id, rider_id, pt, ts])



def process_all_files(source_root, output_root):
    os.makedirs(output_root, exist_ok=True)

    for folder in sorted(os.listdir(source_root)):
        if folder.startswith("Berlin_20") and os.path.isdir(os.path.join(source_root, folder)):
            source_folder_path = os.path.join(source_root, folder)
            target_folder_path = os.path.join(output_root, folder)

            os.makedirs(target_folder_path, exist_ok=True)

            print(f"Folder : {folder} → {target_folder_path}")

            rides_path = os.path.join(target_folder_path, "rides_sql.csv")

            with open(rides_path, 'w', newline='', encoding='utf-8') as ride_file:

                ride_writer = csv.writer(ride_file)

                ride_writer.writerow(["ride_id", "rider_id","geom", "timestamp"])

                for file in os.listdir(source_folder_path):
                    if file == "rides.csv":
                        filepath = os.path.join(source_folder_path, file)
                        try:
                            extract_data(filepath, ride_writer)
                        except Exception as e:
                            print(f"Error in {filepath} : {e}")


if __name__ == "__main__":
    data_root = "./dataMobilityDB/"
    output_dir = "dataSQL"
    process_all_files(data_root, output_dir)
    print(f"Processing complete. Files rides_sql.csv have been generated.")
