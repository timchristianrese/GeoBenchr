#!/usr/bin/env python3
import json
import random
from pathlib import Path
import sys

import numpy as np
import pandas as pd
import torch

# ────────── CONFIGURATION ──────────
NUM_TRAJ        = 20
MAX_LEN         = 120

BASE_DIR        = Path(__file__).resolve().parents[0]
MODEL_PATH      = BASE_DIR / "model_lstm.pt"
STATS_PATH      = BASE_DIR / "dataset" / "stats.json"
FLIGHTS_TXT_DIR = BASE_DIR / "flights_txt"
OUTPUT_DIR      = BASE_DIR / "generated"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

DEVICE = torch.device("mps" if torch.backends.mps.is_available() else "cpu")
print("Generation device :", DEVICE)

sys.path.append(str(BASE_DIR))
from model import FlightLSTM

stats = json.load(open(STATS_PATH))
feat_keys = ["dlat", "dlon", "dalt", "dtime", "gs_kt", "heading"]
mean = torch.tensor([stats[k]["mean"] for k in feat_keys], dtype=torch.float32, device=DEVICE)
std  = torch.tensor([stats[k]["std"]  for k in feat_keys], dtype=torch.float32, device=DEVICE)

def norm(x: torch.Tensor) -> torch.Tensor:
    return (x - mean) / std

def denorm(x: torch.Tensor) -> torch.Tensor:
    return x * std + mean

model = FlightLSTM(feat_dim=6).to(DEVICE)
model.load_state_dict(torch.load(MODEL_PATH, map_location=DEVICE))
model.eval()

real_files = list(FLIGHTS_TXT_DIR.glob("*.txt"))
if not real_files:
    raise RuntimeError(f"No txt file in {FLIGHTS_TXT_DIR}")

def sample_seed():
    df = pd.read_csv(random.choice(real_files))
    first = df.iloc[0]

    start_abs = {
        "lat": float(first["lat"]),
        "lon": float(first["lon"]),
        "alt": float(first["alt_ft"]),
    }

    heading = first["heading_deg"]
    if pd.isna(heading):
        heading = 90.0
    else:
        heading = float(heading)

    seed_raw = torch.tensor([[0.0, 0.0, 0.0, 5.0, 400.0, heading]],
                            dtype=torch.float32, device=DEVICE)
    seed_norm = norm(seed_raw.unsqueeze(0))  # shape (1, 1, 6)
    return seed_norm, start_abs

print(f"Generating {NUM_TRAJ} trajectories…")

for idx in range(NUM_TRAJ):
    seed_norm, start = sample_seed()
    seq = [seed_norm]  # (1, 1, 6)
    inp = seed_norm

    with torch.no_grad():
        for _ in range(MAX_LEN - 1):
            out = model(inp)                   # (1, T, 6)
            next_step = out[:, -1:].clone()    # (1, 1, 6)

            # j'avais des erreurs de size de tensor pour ici
            if next_step.shape[-1] != 6:
                print(f"Avertissement : output shape {next_step.shape}, tronqué à 6")
                next_step = next_step[:, :, :6]

            seq.append(next_step)
            inp = torch.cat([inp, next_step], dim=1)

    seq_denorm = denorm(torch.cat(seq, dim=1).squeeze(0)).cpu().numpy()  # (T, 6)

    abs_lat = np.cumsum(seq_denorm[:, 0]) + start["lat"]
    abs_lon = np.cumsum(seq_denorm[:, 1]) + start["lon"]
    abs_alt = np.cumsum(seq_denorm[:, 2]) + start["alt"]

    df_out = pd.DataFrame({
        "lat": abs_lat,
        "lon": abs_lon,
        "alt_ft": abs_alt,
        "delta_t_s": seq_denorm[:, 3],
        "gs_kt": seq_denorm[:, 4],
        "heading_deg": seq_denorm[:, 5],
    })

    out_path = OUTPUT_DIR / f"generated_flight_{idx:05}.csv"
    df_out.to_csv(out_path, index=False)
    print(" saved", out_path)

print("Generation finished", OUTPUT_DIR)