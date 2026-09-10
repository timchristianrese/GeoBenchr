import csv
import glob
import os
from datetime import datetime

# Input pattern (e.g., all .csv files in the folder)
input_pattern = "../raw/ais/uni*.csv"

# Process each matching file
id = 0
min_id = 0 
for filepath in glob.glob(input_pattern):
    print(f"Processing file: {filepath}")
    output_filename = "../processed/ais/point_" + os.path.basename(filepath)
    
    with open(filepath, 'r', newline='') as infile, open(output_filename, 'w', newline='') as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)

        # Write new header
        writer.writerow(["id", "timestamp", "vessel_id", "wkt", "heading", "speed", "course"])
        first_row = True

        current_vessel_id = None
        vessel_ids = {}
        for row in reader:
            if row[0] == 't':
                continue  # skip original header
            timestamp_ms = int(row[0])
            timestamp = datetime.fromtimestamp(timestamp_ms / 1000).strftime('%Y-%m-%d %H:%M:%S')
            vessel_id = row[1][-6:]  # last 6 characters
            if vessel_id not in vessel_ids:
                vessel_ids[vessel_id] = id
                id += 1
            lon = row[2]
            lat = row[3]
            wkt = f"POINT({lon} {lat})"
            heading = row[4]
            speed = row[5]
            course = row[6]
            if course.endswith(','):
                course = course[:-1]
            if vessel_ids[vessel_id] < min_id:
                print(f"Skipping vessel_id {vessel_id} with id {vessel_ids[vessel_id]} (less than min_id {min_id})")
                exit()
            writer.writerow([vessel_ids[vessel_id], timestamp, vessel_id, wkt, heading, speed, course])
    min_id = id
print("Preprocessing complete.")
