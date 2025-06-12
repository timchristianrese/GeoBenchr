import torch
import json
import os
import networkx as nx
from torch.utils.data import Dataset

"""
This file defines a PyTorch Dataset to train models that predict the next location in a bike trip.  
It uses node sequences from real trips and a city graph to build input/output samples with node features.
"""

class TripSequenceDataset(Dataset):
    def __init__(self, path, seq_len=10, graph_path=None):
        # Desired length of each input sequence for the model
        self.seq_len = seq_len
        self.sequences = []

        # Load the node vocabulary
        base_dir = os.path.dirname(os.path.abspath(__file__))
        vocab_path = os.path.join(base_dir, "..", "data", "training", "node_vocab.json")
        with open(vocab_path) as f:
            vocab = json.load(f)
        self.node_to_index = vocab["node_to_index"]
        self.index_to_node = {}
        for k, v in vocab["index_to_node"].items():
            self.index_to_node[int(k)] = v

        # load graph
        if graph_path is None:
            graph_path = os.path.join(base_dir, "..", "berlin_bike.graphml")
        self.G = nx.read_graphml(graph_path)

        # load trajectories
        with open(path) as f:
            for line in f:
                seq = json.loads(line.strip())
                # skip sequence that are too short
                if len(seq) <= seq_len:
                    continue
                for i in range(len(seq) - seq_len):
                    sub_x = seq[i:i+seq_len]
                    sub_y = seq[i+seq_len]
                    # Check if all nodes in the input sequence and target exist in the vocabulary
                    valid = True
                    for n in sub_x + [sub_y]:
                        if n not in self.node_to_index:
                            valid = False
                            break

                    if valid:
                        self.sequences.append((sub_x, sub_y))

    def __len__(self):
        # Return the number of training samples
        return len(self.sequences)

    def __getitem__(self, idx):
        # Get one training example: a sequence and its next node
        x_nodes, y_node = self.sequences[idx]

        x_features = []
        for node_id in x_nodes:
            nid = self.node_to_index[node_id]
            features = self._get_node_features(node_id)  # [x, y, deg]
            x_features.append([nid] + features)          # [id, x, y, deg]

        target = self.node_to_index[y_node]
        return x_features, target


    def _get_node_features(self, node_id):
        # Extract features from the graph for a given node
        node_data = self.G.nodes[node_id]
        x = float(node_data.get("x", 0))
        y = float(node_data.get("y", 0))
        degree = self.G.degree(node_id)
        return [x, y, degree]
