import os
import sys
import math
import random
from datetime import datetime, timedelta

# (for interface.py)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "generate")))

from interface import TrajectoryGenerator

# (for post_treatment.py)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "postprocess")))

from post_treatment import add_directional_noise, smooth_trajectory

def haversine(lat1, lon1, lat2, lon2):
    """Calcul distance between two GPS point in metters (using Haversine formula)"""
    R = 6371000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    d_phi = math.radians(lat2 - lat1)
    d_lambda = math.radians(lon2 - lon1)

    a = math.sin(d_phi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(d_lambda / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c


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


def save_trajectory_txt(file_path, coords, timestamps):
    """Save trajectory in a txt file as our real data (lat, long) and timestamp"""
    with open(file_path, "w", encoding="utf-8") as f:
        f.write("lat,lon,timeStamp\n")
        for i, ((lat, lon), ts) in enumerate(zip(coords, timestamps), 1):
            # Convert timestamp in ms
            ts_ms = int(ts.timestamp() * 1000)
            f.write(f"{lat},{lon},{ts_ms}\n")


def generate_and_save_trajectory(output_dir, traj_id, noise_magnitude=0.0003, avg_speed_m_s=4.0):
    """Generate a trajectory, adding spatial noise, adding temporal aspect and save in file named VM2_-ID"""
    os.makedirs(output_dir, exist_ok=True)
    generator = TrajectoryGenerator()

    # Raw trajectory
    node_ids = generator.generate()
    coords = generator.node_ids_to_coordinates(node_ids)

    # Adding noise and smooth it
    coords_noisy = add_directional_noise(coords, noise_magnitude=noise_magnitude, segment_length=5)
    coords_noisy = smooth_trajectory(coords_noisy, window_size=3)

    # Add temporal aspect
    timestamps = add_timestamps(coords_noisy, avg_speed_m_s=avg_speed_m_s)

    # Save
    filename = f"VM2_-{traj_id}.txt"
    file_path = os.path.join(output_dir, filename)
    save_trajectory_txt(file_path, coords_noisy, timestamps)

    print(f"Trajectoire sauvegardée : {file_path}")
    return file_path


if __name__ == "__main__":
    output_directory = "./output/all"
    for i in range(1, 11):
        generate_and_save_trajectory(output_directory, traj_id=i)
