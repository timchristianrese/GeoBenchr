import json, math
from pathlib import Path

import numpy as np
import pandas as pd
import torch
from tqdm import tqdm

BASE_DIR = Path(__file__).resolve().parents[1]
SRC_DIR  = BASE_DIR / "flights_txt"
DST_DIR  = BASE_DIR / "dataset" / "tensors"
DST_DIR.mkdir(parents=True, exist_ok=True)

# ─────────── FEATURES ───────────
features = [
    "dlat_m",          # north-south dm
    "dlon_m",          # east-west dm
    "dalt",            # dft
    "dtime",           # dt
    "gs_kt",           # ground speed
    "heading_sin",     # sin(heading)
    "heading_cos",     # cos(heading)
]
# ─────────────────────────────────

stats_acc = {k: [] for k in features}

def process_file(path: Path):
    df = pd.read_csv(path, encoding="ISO-8859-1")


    df["dlat"]  = df["lat"].diff().fillna(0)
    df["dlon"]  = df["lon"].diff().fillna(0)
    df["dalt"]  = df["alt_ft"].diff().fillna(0)
    df["dtime"] = df["delta_t_s"].fillna(0)

    # convert dlat / dlon in meters
    lat_rad = np.radians(df["lat"].values)
    meters_per_deg_lat = 111_132.0 # ≈ m/°
    meters_per_deg_lon = 111_320.0 * np.cos(lat_rad) # ≈ m/°
    df["dlat_m"] = df["dlat"].values * meters_per_deg_lat
    df["dlon_m"] = df["dlon"].values * meters_per_deg_lon

    df["gs_kt"]   = df["gs_kt"].ffill().fillna(0)
    heading       = df["heading_deg"].ffill().fillna(0)
    heading_rad   = np.radians(heading)
    df["heading_sin"] = np.sin(heading_rad)
    df["heading_cos"] = np.cos(heading_rad)

    # save tensor
    tensor = torch.tensor(df[features].values, dtype=torch.float32)
    torch.save(tensor, DST_DIR / f"{path.stem}.pt")

    # for stats
    for i, k in enumerate(features):
        stats_acc[k].append(tensor[:, i].numpy())

files = list(SRC_DIR.glob("*.txt"))
for p in tqdm(files, desc="converting"):
    process_file(p)

stats = {}
for k, arrays in stats_acc.items():
    concat = np.concatenate(arrays)
    stats[k] = {"mean": float(concat.mean()),
                "std":  float(concat.std() + 1e-6)}

(STATS_DIR := BASE_DIR / "dataset").mkdir(exist_ok=True)
json.dump(stats, open(STATS_DIR / "stats.json", "w"), indent=2)

print(f"Pre‑process finished : {len(files)} trajectories in {DST_DIR}")