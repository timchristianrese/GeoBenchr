import os
import osmnx as ox
import networkx as nx
import matplotlib.pyplot as plt


def load_or_build_graph(place="Berlin, Germany", graph_filename="berlin_bike.graphml"):
    current_dir = os.path.dirname(os.path.abspath(__file__))
    graph_path = os.path.join(current_dir, "..", graph_filename)

    # If the graph already exists, load it
    if os.path.exists(graph_path):
        print("Loading existing graph")
        G = ox.load_graphml(graph_path)
        print("graph loaded")
    else:
        # Otherwise, download it from OpenStreetMap
        print("Downloading the graph from OpenStreetMap")
        G = ox.graph_from_place(place, network_type="bike")
        ox.save_graphml(G, graph_path)
    return G

if __name__ == "__main__":
    G = load_or_build_graph()
    print(f"[INFO] Graph : {len(G.nodes)} nodes, {len(G.edges)} vertices")

    # nx.draw(G, with_labels=True)
    # plt.show() trop de temps lol