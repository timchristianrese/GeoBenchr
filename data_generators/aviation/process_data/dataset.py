import json
from pathlib import Path

import pandas as pd
import torch
from torch.utils.data import Dataset

BASE_DIR    = Path(__file__).resolve().parents[1]
TENSOR_DIR  = BASE_DIR / "dataset" / "tensors"
TXT_DIR     = BASE_DIR / "flights_txt_for_training"   # original .txt folders
STATS_PATH  = BASE_DIR / "dataset" / "stats.json"

class FlightDataset(Dataset):
    def __init__(self):
        self.paths = list(TENSOR_DIR.glob("*.pt"))
        stats = json.load(open(STATS_PATH))
        self.mean = torch.tensor([stats[k]["mean"] for k in stats], dtype=torch.float32)
        self.std  = torch.tensor([stats[k]["std"]  for k in stats], dtype=torch.float32)

    # -------- get first lat lon --------
    def _start_latlon(self, stem: str):
        txt_path = TXT_DIR / f"{stem}.txt"
        df0 = pd.read_csv(txt_path, nrows=1)
        return float(df0["lat"].iloc[0]), float(df0["lon"].iloc[0])

    def __len__(self):
        return len(self.paths)

    def __getitem__(self, idx):
        x = torch.load(self.paths[idx]) # shape (T, 7)
        x = (x - self.mean) / self.std
        lat0, lon0 = self._start_latlon(self.paths[idx].stem)
        return x, lat0, lon0 # return normalized tensor with start position

# -------- collate function with padding --------
def collate_pad(batch):
    """batch is a list of (tensor, lat0, lon0)"""
    seqs, lats0, lons0 = zip(*batch)
    lengths  = [s.size(0) for s in seqs]
    max_len  = max(lengths)
    feat_dim = seqs[0].size(1)

    padded = torch.zeros(len(batch), max_len, feat_dim)
    mask   = torch.zeros(len(batch), max_len, dtype=torch.bool)

    for i, seq in enumerate(seqs):
        l = seq.size(0)
        padded[i, :l] = seq
        mask[i, :l]   = 1

    x_in  = padded[:, :-1] # input to model
    y_out = padded[:, 1:] # target for next step prediction
    mask  = mask[:, 1:] # mask to ignore padding in loss

    lat0_tensor = torch.tensor(lats0, dtype=torch.float32)
    lon0_tensor = torch.tensor(lons0, dtype=torch.float32)

    return x_in, y_out, mask, lat0_tensor, lon0_tensor