#!/usr/bin/env python3
import json, math, random, sys, time
from pathlib import Path

import numpy as np
import pandas as pd
import torch
from tqdm import tqdm

import pyarrow as pa
import pyarrow.parquet as pq

from geo_utils import signed_distance_torch


# ── CONFIG ──
NUM_TRAJ = 1_000_000
BATCH_SIZE = 8        # 🔥 tune: 32–256 (start with 64)
MAX_LEN = 1000
MIN_STEPS = 3
MARGIN_M = 1000
CENTER_LAT, CENTER_LON = 51.0, 8.5
MIN_POINTS = 5

BASE = Path(__file__).resolve().parent
MODEL_PATH = BASE / "best_lstm.pt"
STATS_PATH = BASE / "dataset" / "stats.json"
TXT_DIR = BASE / "flights_txt"

OUT_DIR = BASE / "generated_parquet"
OUT_DIR.mkdir(parents=True, exist_ok=True)

DEVICE = torch.device(
    "mps" if torch.backends.mps.is_available()
    else "cpu"
)

sys.path.append(str(BASE))
from model import FlightLSTM


# ── AIRPORTS ──
AIRPORTS = np.array([
    (51.2895, 6.7668),
    (50.8659, 7.1427),
    (51.6141, 8.6163),
], dtype=np.float32)

AP_RADIUS_KM = 1.5
M_PER_DEG_LAT = 111_132.0


def m_per_deg_lon(lat_deg):
    return 111_320.0 * np.cos(np.radians(lat_deg))


def haversine_km_vec(lat, lon, airports):
    # vectorized distance to all airports
    lat1 = np.radians(lat[:, None])
    lon1 = np.radians(lon[:, None])
    lat2 = np.radians(airports[:, 0])
    lon2 = np.radians(airports[:, 1])

    dlat = lat2 - lat1
    dlon = lon2 - lon1

    a = np.sin(dlat/2)**2 + np.cos(lat1)*np.cos(lat2)*np.sin(dlon/2)**2
    return 6371.0 * 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))


# ── NORMALIZATION ──
feat_keys = ["dlat_m","dlon_m","dalt","dtime","gs_kt","heading_sin","heading_cos"]

with open(STATS_PATH) as f:
    stats = json.load(f)

mean = torch.tensor([stats[k]["mean"] for k in feat_keys], dtype=torch.float32, device=DEVICE)
std  = torch.tensor([stats[k]["std"]  for k in feat_keys], dtype=torch.float32, device=DEVICE)

def norm(x): return (x - mean) / std
def denorm(x): return x * std + mean


# ── DATA ──
TXT_FILES = list(TXT_DIR.glob("*.txt"))
TXT_CONTENTS = [pd.read_csv(p) for p in TXT_FILES]


def sample_seed_batch(batch_size):
    seeds = []
    starts = []

    for _ in range(batch_size):
        df = random.choice(TXT_CONTENTS)
        first = df.iloc[0]

        lat = float(first["lat"])
        lon = float(first["lon"])
        alt = float(first["alt_ft"])

        dy_m = (CENTER_LAT - lat) * M_PER_DEG_LAT
        dx_m = (CENTER_LON - lon) * m_per_deg_lon(lat)

        norm_v = math.hypot(dy_m, dx_m) + 1e-6
        dlat1 = dy_m / norm_v * 2000
        dlon1 = dx_m / norm_v * 2000

        heading = math.atan2(dx_m, dy_m)

        seed = [
            [0,0,0,5,400, math.sin(heading), math.cos(heading)],
            [dlat1, dlon1,0,5,400, math.sin(heading), math.cos(heading)],
        ]

        seeds.append(seed)
        starts.append((lat, lon, alt))

    seed = torch.tensor(seeds, dtype=torch.float32, device=DEVICE)
    return norm(seed), starts


# ── MODEL ──
model = FlightLSTM(feat_dim=7).to(DEVICE)
model.load_state_dict(torch.load(MODEL_PATH, map_location=DEVICE))
model.eval()
torch.set_grad_enabled(False)


# ── GENERATION ──
def generate_batch(batch_size):
    seed_batch, starts = sample_seed_batch(batch_size)

    inp = seed_batch
    seq = [seed_batch]

    cur_lat = np.array([s[0] for s in starts], dtype=np.float32)
    cur_lon = np.array([s[1] for s in starts], dtype=np.float32)
    cur_alt = np.array([s[2] for s in starts], dtype=np.float32)

    lat_hist = [[v] for v in cur_lat]
    lon_hist = [[v] for v in cur_lon]

    alive = np.ones(batch_size, dtype=bool)

    for step in range(MAX_LEN - 2):

        # 🔥 single forward pass (good)
        mu = model(inp)[:, -1:]
        next_step = mu + 0.05 * torch.randn_like(mu)

        seq.append(next_step)
        inp = torch.cat([inp, next_step], dim=1)

        step_denorm = denorm(next_step[:, 0]).detach().cpu().numpy()

        dlat = step_denorm[:, 0]
        dlon = step_denorm[:, 1]

        # 🔥 vector update (cheap)
        cur_lat[alive] += dlat[alive] / M_PER_DEG_LAT
        cur_lon[alive] += dlon[alive] / m_per_deg_lon(cur_lat[alive])

        # append only alive (cheap Python, not torch-heavy)
        for i in np.where(alive)[0]:
            lat_hist[i].append(cur_lat[i])
            lon_hist[i].append(cur_lon[i])

        # 🔥 only occasional expensive checks
        if step % 5 == 0:

            lat_t = torch.tensor(cur_lat[alive], dtype=torch.float32, device=DEVICE).unsqueeze(1)
            lon_t = torch.tensor(cur_lon[alive], dtype=torch.float32, device=DEVICE).unsqueeze(1)

            dist = signed_distance_torch(lat_t, lon_t).cpu().numpy().flatten()

            alive_idx = np.where(alive)[0]
            alive[alive_idx[dist > MARGIN_M]] = False

            if not alive.any():
                break

    results = []

    for i in range(batch_size):
        if len(lat_hist[i]) < MIN_POINTS:
            continue

        results.append(pd.DataFrame({
            "lat": lat_hist[i],
            "lon": lon_hist[i],
        }))

    return results


# ── MAIN ──
def main():
    print(f"Device: {DEVICE}")
    print(f"Batch size: {BATCH_SIZE}")

    batch = []
    batch_rows = 0
    batch_id = 0

    FLUSH_ROWS = 100_000
    last_flush = time.time()

    total = 0

    pbar = tqdm(total=NUM_TRAJ)

    while total < NUM_TRAJ:
        dfs = generate_batch(BATCH_SIZE)

        for df in dfs:
            batch.append(df)
            batch_rows += len(df)
            total += 1
            pbar.update(1)

        if batch_rows >= FLUSH_ROWS or (time.time() - last_flush) > 40:
            df = pd.concat(batch, ignore_index=True)
            table = pa.Table.from_pandas(df)

            out = OUT_DIR / f"generated_{batch_id:05d}.parquet"
            pq.write_table(table, out, compression="snappy")

            print(f"[WRITE] {out}")

            batch = []
            batch_rows = 0
            batch_id += 1
            last_flush = time.time()

    pbar.close()
    print("✅ Done")


if __name__ == "__main__":
    main()