import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import json
import torch
import random
import numpy as np
import networkx as nx
from torch.nn.functional import softmax
from models.lstm_traj_model import LSTMTripPredictor

"""
This file defines a class to generate realistic bike trip trajectories using a trained LSTM model and a city graph.  
It predicts the next location step-by-step based on previous nodes and graph features.
"""

class TrajectoryGenerator:
    def __init__(self, seq_len=10, embedding_dim=64, hidden_dim=128, max_len=50):
        base_dir = os.path.dirname(os.path.abspath(__file__))
        self.graph_path = os.path.join(base_dir, "..", "berlin_bike.graphml")
        self.model_path = os.path.join(base_dir, "..", "models", "trained_lstm_model.pt")
        self.vocab_path = os.path.join(base_dir, "..", "data", "training", "node_vocab.json")

        # model config
        self.seq_len = seq_len
        self.embedding_dim = embedding_dim
        self.hidden_dim = hidden_dim
        self.max_len = max_len
        self.device = 'cuda' if torch.cuda.is_available() else 'cpu'

        self._load_resources()

    def _load_resources(self):
        # Load graph from file
        self.G = nx.read_graphml(self.graph_path)

        # Load node vocabulary
        with open(self.vocab_path) as f:
            vocab = json.load(f)
        self.node_to_index = vocab['node_to_index']
        self.index_to_node = {int(k): v for k, v in vocab['index_to_node'].items()}
        self.vocab_size = len(self.node_to_index)

        # Load trained model
        self.model = LSTMTripPredictor(self.vocab_size, self.embedding_dim, self.hidden_dim).to(self.device)
        self.model.load_state_dict(torch.load(self.model_path, map_location=self.device))
        self.model.eval()

    def _get_node_features(self, node_id):
        node_data = self.G.nodes[node_id]
        x = float(node_data.get("x", 0))
        y = float(node_data.get("y", 0))
        deg = self.G.degree(node_id)
        return [x, y, deg]

    def generate(self, max_len=30):
        self.model.eval()

        valid_starts = [n for n in self.node_to_index if any(
            neighbor in self.node_to_index for neighbor in self.G.neighbors(n)
        )]
        if not valid_starts:
            print("No starting node available")
            return []

        start_node = np.random.choice(valid_starts)
        print(f"Starting on node {start_node}")

        node_ids = [start_node]
        visited = set([start_node])

        for step in range(max_len):
            input_seq = node_ids[-self.seq_len:]
            features_seq = []

            for nid in input_seq:
                feat = self._get_node_features(nid)  # [x, y, deg]
                node_idx = self.node_to_index[nid]
                features_seq.append([node_idx] + feat)

            # Padding if necessary
            while len(features_seq) < self.seq_len:
                features_seq.insert(0, [0.0, 0.0, 0.0, 0.0])

            input_tensor = torch.tensor([features_seq], dtype=torch.float32).to(self.device)

            with torch.no_grad():
                logits = self.model(input_tensor)
                probs = torch.softmax(logits[0], dim=-1).cpu().numpy()

            current_node = node_ids[-1]
            neighbors = [n for n in self.G.neighbors(current_node) if n in self.node_to_index]

            if not neighbors:
                print(f"No neighbor for node : {current_node}")
                break

            neighbor_indices = [self.node_to_index[n] for n in neighbors]
            neighbor_probs = probs[neighbor_indices]
            if neighbor_probs.sum() == 0:
                break
            neighbor_probs /= neighbor_probs.sum()

            next_index = np.random.choice(neighbor_indices, p=neighbor_probs)
            next_node = self.index_to_node[next_index]

            if next_node in visited:
                continue
            visited.add(next_node)
            node_ids.append(next_node)

        return node_ids

    def node_ids_to_coordinates(self, node_ids):
        coords = []
        for node_id in node_ids:
            if node_id in self.G.nodes:
                data = self.G.nodes[node_id]
                if 'x' in data and 'y' in data:
                    coords.append((float(data['y']), float(data['x'])))
                else:
                    print(f"Be careful: node {node_id} does not have GPS coord")
            else:
                print(f"Be careful : node {node_id} is not in the graph")

        return coords
