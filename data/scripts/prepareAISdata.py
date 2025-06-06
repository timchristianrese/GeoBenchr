import csv
import glob
import os
from datetime import datetime

# Input pattern (e.g., all .csv files in the folder)
input_pattern = "../raw/ais/uni*.csv"

# Process each matching file
for filepath in glob.glob(input_pattern):
    output_filename = "../processed/ais/point_" + os.path.basename(filepath)
    
    with open(filepath, 'r', newline='') as infile, open(output_filename, 'w', newline='') as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)

        # Write new header
        writer.writerow(["timestamp", "vessel_id", "wkt", "heading", "speed", "course"])
        
        for row in reader:
            if row[0] == 't':
                continue  # skip original header

            timestamp_ms = int(row[0])
            timestamp = datetime.fromtimestamp(timestamp_ms / 1000).strftime('%Y-%m-%d %H:%M:%S')
            vessel_id = row[1][-6:]  # last 6 characters
            lon = row[2]
            lat = row[3]
            wkt = f"POINT({lon} {lat})"
            heading = row[4]
            speed = row[5]
            course = row[6]
            if course.endswith(','):
                course = course[:-1]
            writer.writerow([timestamp, vessel_id, wkt, heading, speed, course])

print("Preprocessing complete.")
