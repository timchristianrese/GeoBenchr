# model2/generate/map_display.py

import folium
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "postprocess")))

from interface import TrajectoryGenerator
from post_treatment import add_directional_noise, smooth_trajectory

generator = TrajectoryGenerator()

N_TRAJETS = 10
output_file = "./output/generated_map_with_noise.html"
os.makedirs(os.path.dirname(output_file), exist_ok=True)

m = folium.Map(location=[52.52, 13.405], zoom_start=13)

def plot_trajectory(coords, color, tooltip):
    if len(coords) > 1:
        folium.PolyLine(coords, color=color, weight=2, opacity=0.7, tooltip=tooltip).add_to(m)
        folium.Marker(coords[0], tooltip=f"Départ {tooltip}", icon=folium.Icon(color="green")).add_to(m)
        folium.Marker(coords[-1], tooltip=f"Arrivée {tooltip}", icon=folium.Icon(color="red")).add_to(m)
        return True
    return False

nb_success_raw = 0
nb_success_noisy = 0

print("=== Generation ===")
for i in range(N_TRAJETS):
    print(f"nb {i + 1}/{N_TRAJETS}")
    try:
        node_ids = generator.generate()
        coords = generator.node_ids_to_coordinates(node_ids)
        if plot_trajectory(coords, color="blue", tooltip=f"Brut {i+1}"):
            nb_success_raw += 1
        else:
            print("too short, ignored")
    except Exception as e:
        print(f"error when generating {i + 1} : {e}")

print("\n=== Generating noisy traj ===")
for i in range(N_TRAJETS):
    try:
        node_ids = generator.generate()
        coords = generator.node_ids_to_coordinates(node_ids)

        coords_noisy = add_directional_noise(coords, noise_magnitude=0.0003, segment_length=5)
        coords_noisy = smooth_trajectory(coords_noisy, window_size=3)

        if plot_trajectory(coords_noisy, color="red", tooltip=f"Bruit {i+1}"):
            nb_success_noisy += 1
        else:
            print("traj too short, ignored")
    except Exception as e:
        print(f"error when generating {i + 1} : {e}")

# Sauvegarde
if nb_success_raw + nb_success_noisy == 0:
    print("no valid traj")
else:
    m.save(output_file)
    print(f"{nb_success_raw} raw traj displayed on map")
    print(f"{nb_success_noisy} noisy traj displayed on map")
    print(f"Saved map : {output_file}")
