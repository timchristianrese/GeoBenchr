import numpy as np

def add_directional_noise(coords, noise_magnitude=0.00015, segment_length=5):
    noisy_coords = coords.copy()
    if len(coords) < segment_length + 2:
        # too short
        return noisy_coords

    start_idx = np.random.randint(0, len(coords) - segment_length - 1)
    end_idx = start_idx + segment_length

    noise_values = np.linspace(noise_magnitude, -noise_magnitude, segment_length)
    noise_values = noise_values + np.random.normal(scale=noise_magnitude*0.2, size=segment_length)

    for i in range(start_idx, end_idx):
        prev_pt = np.array(coords[i-1])
        curr_pt = np.array(coords[i])
        direction = curr_pt - prev_pt
        direction /= np.linalg.norm(direction) + 1e-8

        orthogonal = np.array([-direction[1], direction[0]])

        offset = orthogonal * noise_values[i - start_idx]

        noisy_coords[i] = tuple(curr_pt + offset)

    return noisy_coords

def smooth_trajectory(coords, window_size=3):
    smoothed = []
    for i in range(len(coords)):
        start = max(0, i - window_size//2)
        end = min(len(coords), i + window_size//2 + 1)
        segment = coords[start:end]
        avg_lat = np.mean([pt[0] for pt in segment])
        avg_lon = np.mean([pt[1] for pt in segment])
        smoothed.append((avg_lat, avg_lon))
    return smoothed
