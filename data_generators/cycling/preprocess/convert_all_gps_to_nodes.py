import json
import os
from shapely.geometry import LineString
from scipy.spatial import KDTree
import networkx as nx


"""
This script processes raw GPS bike trips by simplifying them, matching points to a street graph,  
and generating clean node sequences using shortest paths. The output is used for model training.
"""

# Simplify a GPS trajectory (remove useless point)
def simplify_gps_traj(points, tolerance=5):
    line = LineString(points)
    return list(line.simplify(tolerance).coords)

# Build a KDTree for fast nearest neighbor search in the graph
def build_kdtree(G):
    node_positions = {
        node: (float(data['x']), float(data['y']))
        for node, data in G.nodes(data=True)
        if 'x' in data and 'y' in data
    }
    coords = list(node_positions.values())
    node_ids = list(node_positions.keys())
    tree = KDTree(coords)
    return tree, coords, node_ids

# Match each GPS point to the closest node in the graph
def match_gps_to_graph(traj, tree, coords, node_ids):
    matched_nodes = []
    for lon, lat in traj:
        dist, idx = tree.query((lon, lat))
        matched_nodes.append(node_ids[idx])
    return matched_nodes

# Main function to process all GPS trips
def process_all_trips():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    graph_path = os.path.join(base_dir, "..", "berlin_bike.graphml")
    raw_dir = os.path.join(base_dir, "..", "data", "raw_trips", "human")
    out_dir = os.path.join(base_dir, "..", "data", "processed", "human")

    os.makedirs(out_dir, exist_ok=True)

    print("Loading graph")
    G = nx.read_graphml(graph_path)
    tree, coords, node_ids = build_kdtree(G)

    print("journey treatment")
    count = 0
    for filename in os.listdir(raw_dir):
        if not filename.endswith(".json"):
            continue

        trip_path = os.path.join(raw_dir, filename)
        with open(trip_path, 'r') as f:
            gps_points = json.load(f)

        if len(gps_points) < 2:
            continue

        simplified = simplify_gps_traj(gps_points, tolerance=10)
        matched = match_gps_to_graph(simplified, tree, coords, node_ids)

        full_path = []
        for i in range(len(matched) - 1):
            try:
                path_segment = nx.shortest_path(G, matched[i], matched[i+1])
                if full_path:
                    full_path.extend(path_segment[1:])
                else:
                    full_path.extend(path_segment)
            except nx.NetworkXNoPath:
                print(f"No path found between {matched[i]} and {matched[i+1]}")

        if len(full_path) >= 5:
            output_path = os.path.join(out_dir, filename)
            with open(output_path, 'w') as f:
                json.dump(full_path, f)
            print(f"{filename} → {len(full_path)} nodes")
            count += 1
        else:
            print(f"{filename} ignored (could be too short ou weird (like with gap in))")

    print(f"Valid trajectory saved : {count}")

if __name__ == "__main__":
    process_all_trips()
