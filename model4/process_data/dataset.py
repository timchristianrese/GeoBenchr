import json
from pathlib import Path

import torch
from torch.utils.data import Dataset

BASE_DIR    = Path(__file__).resolve().parents[1]
TENSOR_DIR  = BASE_DIR / "dataset" / "tensors"
STATS_PATH  = BASE_DIR / "dataset" / "stats.json"

class FlightDataset(Dataset):
    def __init__(self):
        self.paths = list(TENSOR_DIR.glob("*.pt"))
        stats = json.load(open(STATS_PATH))
        self.mean = torch.tensor([stats[k]["mean"] for k in stats], dtype=torch.float32)
        self.std  = torch.tensor([stats[k]["std"]  for k in stats], dtype=torch.float32)

    def __len__(self):
        return len(self.paths)

    def __getitem__(self, idx):
        x = torch.load(self.paths[idx])
        x = (x - self.mean) / self.std
        return x

def collate_pad(batch):
    lengths  = [seq.size(0) for seq in batch]
    max_len  = max(lengths)
    feat_dim = batch[0].size(1)

    padded = torch.zeros(len(batch), max_len, feat_dim)
    mask   = torch.zeros(len(batch), max_len, dtype=torch.bool)

    for i, seq in enumerate(batch):
        l = seq.size(0)
        padded[i, :l] = seq
        mask[i, :l] = 1

    x_in  = padded[:, :-1]
    y_out = padded[:, 1:]
    mask  = mask[:, 1:]

    return x_in, y_out, mask