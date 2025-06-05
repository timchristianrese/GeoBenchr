import os
import math
import random
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

from generate.interface import TrajectoryGenerator
from postprocess.post_treatment import add_directional_noise, smooth_trajectory

# Parameters
NUM_TRAJ = 2000
START_TRIP_ID = 2000
NUM_RIDERS = 60000
THREADS = 8
OUTPUT_DIR = "./output/all"

# Calculate distance between two GPS points using Haversine formula
def haversine(lat1, lon1, lat2, lon2):
    R = 6371000  # Earth radius in meters
    phi1, phi2 = map(math.radians, (lat1, lat2))
    d_phi = math.radians(lat2 - lat1)
    d_lambda = math.radians(lon2 - lon1)
    a = math.sin(d_phi / 2)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(d_lambda / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c

# Create a list of timestamps for each coordinate
def add_timestamps(coords, avg_speed_m_s=4.0):
    """Add a temporal dimension by estimating timestamp for each point"""
    timestamps = []
    t0 = datetime.now()
    timestamps.append(t0)
    for i in range(1, len(coords)):
        dist = haversine(coords[i - 1][0], coords[i - 1][1], coords[i][0], coords[i][1])
        dt = dist / avg_speed_m_s
        dt = dt * (0.9 + 0.2 * random.random())  # temportal noise (around 10%)
        t0 = t0 + timedelta(seconds=dt)
        timestamps.append(t0)
    return timestamps

# Save trajectory to text file with header (trip_id and rider_id)
def save_trajectory_txt(file_path, coords, timestamps, trip_id, rider_id):
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(f"trip_id: {trip_id}\n")
        f.write(f"rider_id: {rider_id}\n")
        f.write("lat,lon,timeStamp\n")
        for (lat, lon), ts in zip(coords, timestamps):
            ts_ms = int(ts.timestamp() * 1000)
            f.write(f"{lat},{lon},{ts_ms}\n")

def generate_biased_rider_ids(num_trips, num_riders=60000):
    high_activity = int(0.01 * num_riders)    # top 1%
    medium_activity = int(0.05 * num_riders)  # next 5%
    low_activity = num_riders - high_activity - medium_activity

    rider_ids = []

    high_trips = int(0.5 * num_trips)
    medium_trips = int(0.3 * num_trips)
    low_trips = num_trips - high_trips - medium_trips

    for _ in range(high_trips):
        rider_ids.append(random.randint(0, high_activity - 1))

    for _ in range(medium_trips):
        rider_ids.append(random.randint(high_activity, high_activity + medium_activity - 1))

    for _ in range(low_trips):
        rider_ids.append(random.randint(high_activity + medium_activity, num_riders - 1))

    random.shuffle(rider_ids)
    return rider_ids

# Generate a single trajectory with noise and timestamps
def generate_single_trajectory(trip_id, rider_id, generator, output_dir, noise_magnitude=0.0003, avg_speed_m_s=4.0):
    try:
        node_ids = generator.generate()
        coords = generator.node_ids_to_coordinates(node_ids)
        coords_noisy = add_directional_noise(coords, noise_magnitude=noise_magnitude, segment_length=5)
        coords_noisy = smooth_trajectory(coords_noisy, window_size=3)
        timestamps = add_timestamps(coords_noisy, avg_speed_m_s=avg_speed_m_s)

        filename = f"VM2_-{trip_id}.txt"
        file_path = os.path.join(output_dir, filename)
        save_trajectory_txt(file_path, coords_noisy, timestamps, trip_id, rider_id)
        return f"[OK] Trajectory {trip_id} (rider {rider_id}) saved."
    except Exception as e:
        return f"[ERROR] Trajectory {trip_id} failed: {e}"

# Main
if __name__ == "__main__":
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    generator = TrajectoryGenerator()

    trip_ids = list(range(START_TRIP_ID, START_TRIP_ID + NUM_TRAJ))
    rider_ids = generate_biased_rider_ids(NUM_TRAJ, NUM_RIDERS)

    print(f"Generating {NUM_TRAJ} trajectories using {THREADS} threads...")

    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        futures = [
            executor.submit(generate_single_trajectory, trip_id, rider_id, generator, OUTPUT_DIR)
            for trip_id, rider_id in zip(trip_ids, rider_ids)
        ]

        for future in as_completed(futures):
            print(future.result())

    print("Trajectory generation finished.")
