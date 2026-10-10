#!/usr/bin/env python3
"""Generate trajectories with a trained LSTM
1. Two initial steps oriented toward the center of the area
2. Stop when the path exits the region (>100 m) or reaches an airport (≤3 km)
3. Discard a path if it has fewer than MIN_POINTS points
"""
import json
import math
import random
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import torch
from geo_utils import signed_distance_torch

# ── CONFIG ──
NUM_TRAJ   = 500      # number of files to keep
MAX_LEN    = 300      # max time steps (including the 2 seeded steps)
MIN_STEPS  = 3        # do not test border before this index (0‑based)
MARGIN_M   = 100      # tolerated distance outside the region (m)
CENTER_LAT, CENTER_LON = 51.0, 8.5  # center of the region
MIN_POINTS = 5        # keep a path only if it has at least MIN_POINTS rows

BASE = Path(__file__).resolve().parent
MODEL_PATH = BASE / "best_lstm.pt"
STATS_PATH = BASE / "dataset" / "stats.json"
TXT_DIR    = BASE / "flights_txt"
OUT_DIR    = BASE / "generated"
OUT_DIR.mkdir(parents=True, exist_ok=True)

DEVICE = torch.device("mps" if torch.backends.mps.is_available() else "cpu")
print("Generation device", DEVICE)

sys.path.append(str(BASE))
from model import FlightLSTM

# ── normalisation ──
feat_keys = ["dlat_m", "dlon_m", "dalt", "dtime", "gs_kt", "heading_sin", "heading_cos"]
with open(STATS_PATH) as f:
    stats = json.load(f)
mean = torch.tensor([stats[k]["mean"] for k in feat_keys], dtype=torch.float32, device=DEVICE)
std  = torch.tensor([stats[k]["std"]  for k in feat_keys], dtype=torch.float32, device=DEVICE)

def norm(x: torch.Tensor) -> torch.Tensor:
    return (x - mean) / std

def denorm(x: torch.Tensor) -> torch.Tensor:
    return x * std + mean

# ── model ──
model = FlightLSTM(feat_dim=7).to(DEVICE)
model.load_state_dict(torch.load(MODEL_PATH, map_location=DEVICE))
model.eval()

# ── airports in the region ──
AIRPORTS = {
    "EDDL": (51.2895, 6.7668),
    "EDDK": (50.8659, 7.1427),
    "EDLP": (51.6141, 8.6163),
}
AP_RADIUS_KM = 1.5

# ── helpers ──
M_PER_DEG_LAT = 111_132.0

def m_per_deg_lon(lat_deg: float) -> float:
    return 111_320.0 * math.cos(math.radians(lat_deg))

def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371.0
    dlat, dlon = map(math.radians, [lat2 - lat1, lon2 - lon1])
    a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

# ── seed with two initial steps toward the center ──
TXT_FILES = list(TXT_DIR.glob("*.txt"))
if not TXT_FILES:
    print("No txt in", TXT_DIR)
    sys.exit(1)

def sample_seed():
    df = pd.read_csv(random.choice(TXT_FILES))
    first = df.iloc[0]
    start = {
        "lat": float(first["lat"]),
        "lon": float(first["lon"]),
        "alt": float(first["alt_ft"]),
    }

    dy_m = (CENTER_LAT - start["lat"]) * M_PER_DEG_LAT
    dx_m = (CENTER_LON - start["lon"]) * m_per_deg_lon(start["lat"])
    norm_v = math.hypot(dy_m, dx_m) + 1e-6
    step_len = 2000.0  # 2 km per seeded step
    dlat1 = dy_m / norm_v * step_len
    dlon1 = dx_m / norm_v * step_len
    heading_rad = math.atan2(dx_m, dy_m)

    seed_raw = torch.tensor([
        [0.0,   0.0,   0.0, 5.0, 400.0, math.sin(heading_rad), math.cos(heading_rad)],
        [dlat1, dlon1, 0.0, 5.0, 400.0, math.sin(heading_rad), math.cos(heading_rad)],
    ], dtype=torch.float32, device=DEVICE)

    return norm(seed_raw).unsqueeze(0), start  # shape (1,2,7)

# ── generation ──
print("Target", NUM_TRAJ, "valid trajectories")

generated = 0
attempt = 0
while generated < NUM_TRAJ:
    attempt += 1
    seed_norm, start = sample_seed()
    seq = [seed_norm] # list of tensors (1, t, 7)
    inp = seed_norm # current input to the model

    with torch.no_grad():
        for step in range(MAX_LEN - 2):  # 2 steps already provided
            mu = model(inp)[:, -1:] # prediction (B,1,7)
            next_step = mu + 0.05 * torch.randn_like(mu)
            seq.append(next_step)
            inp = torch.cat([inp, next_step], dim=1)

            # reconstruct absolute position of the last point
            denorm_all = denorm(torch.cat(seq, dim=1))[0]  # (t,7)
            dlat_m = denorm_all[:, 0].cpu().numpy()
            dlon_m = denorm_all[:, 1].cpu().numpy()
            lat_deg = np.cumsum(dlat_m / M_PER_DEG_LAT) + start["lat"]
            lon_deg = start["lon"] + np.cumsum([
                dlon_m[i] / m_per_deg_lon(lat_deg[i]) for i in range(len(dlon_m))
            ])
            cur_lat, cur_lon = lat_deg[-1], lon_deg[-1]

            # border check after MIN_STEPS
            if step + 2 >= MIN_STEPS:
                dist_border = signed_distance_torch(
                    torch.tensor([[cur_lat]], dtype=torch.float32, device=DEVICE),
                    torch.tensor([[cur_lon]], dtype=torch.float32, device=DEVICE),
                ).item()
                if dist_border > MARGIN_M:
                    break  # outside region

            # airport check
            hit_ap = any(haversine_km(cur_lat, cur_lon, a, o) < AP_RADIUS_KM for a, o in AIRPORTS.values())
            if hit_ap:
                break

    # final length
    denorm_seq = denorm(torch.cat(seq, dim=1).squeeze(0)).cpu().numpy()
    if len(denorm_seq) < MIN_POINTS:
        print("attempt", attempt, "discarded, only", len(denorm_seq), "pts")
        continue

    # absolute lat lon arrays match seq length
    lat_out = lat_deg[: len(denorm_seq)]
    lon_out = lon_deg[: len(denorm_seq)]

    df = pd.DataFrame({
        "lat": lat_out,
        "lon": lon_out,
        "alt_ft": np.cumsum(denorm_seq[:, 2]) + start["alt"],
        "delta_t_s": denorm_seq[:, 3],
        "gs_kt": denorm_seq[:, 4],
        "heading_deg": (np.degrees(np.arctan2(denorm_seq[:, 5], denorm_seq[:, 6])) + 360) % 360,
    })

    out = OUT_DIR / f"generated_flight_{generated:03}.txt"
    df.to_csv(out, index=False)
    generated += 1
    print("saved", out, f"({len(df)} pts)", "total", generated, "/", NUM_TRAJ)

print("Generation finished", OUT_DIR)