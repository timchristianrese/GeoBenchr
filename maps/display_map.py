import folium
import os

def get_all_ids_from_folder(folder_path, max_files=None):
    """
    extract id from journey files VM2_-<id>
    """
    file_list = [
        filename for filename in os.listdir(folder_path)
        if filename.startswith("VM2_-")
    ]

    file_list = sorted(file_list)

    if max_files is not None:
        return file_list[:max_files]
    return file_list

def extract_coordinates(filepath):
    coords = []
    in_gps_block = False  # used to ensure that we are reading gps datas of the ride

    with open(filepath, 'r') as f:
        for line in f:
            line = line.strip()

            # find the separation between block
            if line.startswith('='):
                in_gps_block = True
                continue

            if not in_gps_block:
                continue  # for the moment, we ignore everything before "="

            # gps lines
            if line and not line.startswith(','):
                parts = line.split(',')
                try:
                    lat = float(parts[0])
                    lon = float(parts[1])
                    coords.append((lat, lon))
                except (ValueError, IndexError):
                    continue
    return coords

def plot_multiple_trajectories_on_map(filepaths, base_data_path, output_file="map_all_trajets.html", startEnd = True):
    all_coords = []
    trajets = []

    for path in filepaths:
        full_path = base_data_path + path
        coords = extract_coordinates(full_path)
        if coords:
            trajets.append(coords)
            all_coords.extend(coords)

    if not all_coords:
        print("No coords found")
        return

    # center map on first point
    m = folium.Map(location=all_coords[0], zoom_start=14)

    for i, coords in enumerate(trajets):
        folium.PolyLine(coords, color="blue", weight=2.5, tooltip=f"Trajet {i+1}").add_to(m)
        if(startEnd):
        # used to know where start and end are
            folium.Marker(coords[0], tooltip=f"Départ {i+1}", icon=folium.Icon(color='green')).add_to(m)
            folium.Marker(coords[-1], tooltip=f"Arrivée {i+1}", icon=folium.Icon(color='red')).add_to(m)

    m.save(output_file)
    print(f"Carte enregistrée sous : {output_file}")


"""
Example of use for 1 journey
"""
# base_data_path = "../data_files/Berlin/Rides/"
# file_path = f"VM2_-{id}"
# coords = extract_coordinates(base_data_path + file_path)
# plot_coordinates_on_map(coords, output_file=f"./output/trajet_VM2_{id}.html")


"""
Example of use for multiple journey
"""
# base_data_path = "../data_files/Berlin/Rides/"
# file_list = ["VM2_-1088851", "VM2_-173773", "VM2_-1474455"]
# plot_multiple_trajectories_on_map(file_list, base_data_path, output_file="./output/all_trajets.html")

"""
Example of use for multiple journey and by extracting id from files name
"""

# base_data_path = "../data_files/Berlin/Rides/"
# file_list = get_all_ids_from_folder(base_data_path, 200)

# # print 20 first
# print(file_list[:20])

# plot_multiple_trajectories_on_map(file_list, base_data_path, output_file="./output/all_trajets.html", startEnd=False)


