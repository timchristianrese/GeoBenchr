import os
import math
import random
from datetime import datetime, timedelta
from concurrent.futures import ProcessPoolExecutor, as_completed

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from postprocess.post_treatment import add_directional_noise, smooth_trajectory
import sys
from pathlib import Path

# -----------------------------
# Setup paths
# -----------------------------
BASE_DIR = Path(__file__).resolve().parents[0]
MODULE_DIR = BASE_DIR / "datasets" / "generate"
sys.path.append(str(MODULE_DIR))

from interface import TrajectoryGenerator

# -----------------------------
# Parameters
# -----------------------------
NUM_TRAJ = 50_000_000
START_TRIP_ID = 2000
NUM_RIDERS = 60000
WORKERS = int(sys.argv[1]) if len(sys.argv) > 1 else 4

OUTPUT_DIR = "./output/parquet"
os.makedirs(OUTPUT_DIR, exist_ok=True)

TRAJ_PER_BATCH = 125000   # controls memory + file size

# -----------------------------
# Utils
# -----------------------------
def haversine(lat1, lon1, lat2, lon2):
    R = 6371000
    phi1, phi2 = map(math.radians, (lat1, lat2))
    d_phi = math.radians(lat2 - lat1)
    d_lambda = math.radians(lon2 - lon1)

    a = math.sin(d_phi / 2)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(d_lambda / 2)**2
    return 2 * R * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def add_timestamps(coords, avg_speed_m_s=4.0):
    timestamps = []
    t0 = datetime.now()
    timestamps.append(t0)

    for i in range(1, len(coords)):
        dist = haversine(coords[i - 1][0], coords[i - 1][1],
                         coords[i][0], coords[i][1])
        dt = dist / avg_speed_m_s
        dt *= (0.9 + 0.2 * random.random())
        t0 = t0 + timedelta(seconds=dt)
        timestamps.append(t0)

    return timestamps


def generate_biased_rider_ids(num_trips, num_riders=60000):
    high_activity = int(0.01 * num_riders)
    medium_activity = int(0.05 * num_riders)

    rider_ids = []

    high_trips = int(0.5 * num_trips)
    medium_trips = int(0.3 * num_trips)

    for _ in range(high_trips):
        rider_ids.append(random.randint(0, high_activity - 1))

    for _ in range(medium_trips):
        rider_ids.append(random.randint(high_activity, high_activity + medium_activity - 1))

    for _ in range(num_trips - high_trips - medium_trips):
        rider_ids.append(random.randint(high_activity + medium_activity, num_riders - 1))

    random.shuffle(rider_ids)
    return rider_ids


# -----------------------------
# Batch generator (LAZY)
# -----------------------------
def generate_trip_batches(start_trip_id, num_traj, batch_size, num_riders):
    trip_id = start_trip_id
    end_id = start_trip_id + num_traj

    while trip_id < end_id:
        current_size = min(batch_size, end_id - trip_id)

        trip_ids = list(range(trip_id, trip_id + current_size))
        rider_ids = generate_biased_rider_ids(current_size, num_riders)

        yield trip_ids, rider_ids

        trip_id += current_size


# -----------------------------
# Worker function
# -----------------------------
def process_batch(args):
    batch_id, trip_ids, rider_ids = args

    generator = TrajectoryGenerator()
    rows = []

    for trip_id, rider_id in zip(trip_ids, rider_ids):
        try:
            node_ids = generator.generate()
            coords = generator.node_ids_to_coordinates(node_ids)

            coords = add_directional_noise(coords, noise_magnitude=0.0003, segment_length=5)
            coords = smooth_trajectory(coords, window_size=3)

            timestamps = add_timestamps(coords)

            for (lat, lon), ts in zip(coords, timestamps):
                rows.append({
                    "trip_id": trip_id,
                    "rider_id": rider_id,
                    "lat": lat,
                    "lon": lon,
                    "timestamp": int(ts.timestamp() * 1000)
                })

        except Exception as e:
            print(f"[ERROR] Trip {trip_id} failed: {e}")

    if rows:
        df = pd.DataFrame(rows)

        file_path = os.path.join(OUTPUT_DIR, f"trajectories_batch_{batch_id}.parquet")

        pq.write_table(
            pa.Table.from_pandas(df),
            file_path,
            compression="snappy"
        )

        return f"[WRITE] Batch {batch_id} → {len(df)} rows"

    return f"[EMPTY] Batch {batch_id}"


# -----------------------------
# Main
# -----------------------------
if __name__ == "__main__":
    print(f"🚀 Starting generation of {NUM_TRAJ} trajectories with {WORKERS} processes")

    batch_iter = generate_trip_batches(
        START_TRIP_ID,
        NUM_TRAJ,
        TRAJ_PER_BATCH,
        NUM_RIDERS
    )

    futures = []
    submitted = 0
    completed = 0

    with ProcessPoolExecutor(max_workers=WORKERS) as executor:

        # Submit initial jobs (bounded queue)
        for _ in range(WORKERS * 2):
            try:
                trip_ids, rider_ids = next(batch_iter)
                futures.append(
                    executor.submit(process_batch, (submitted, trip_ids, rider_ids))
                )
                print(f"[SUBMIT] Batch {submitted}")
                submitted += 1
            except StopIteration:
                break

        # Process dynamically
        while futures:
            for future in as_completed(futures):
                futures.remove(future)
                completed += 1

                try:
                    print(future.result())
                except Exception as e:
                    print(f"[FAIL] {e}")

                # Submit next batch
                try:
                    trip_ids, rider_ids = next(batch_iter)
                    futures.append(
                        executor.submit(process_batch, (submitted, trip_ids, rider_ids))
                    )
                    print(f"[SUBMIT] Batch {submitted}")
                    submitted += 1
                except StopIteration:
                    continue

                print(f"[PROGRESS] Completed {completed} batches")

    print("✅ All trajectories generated.")