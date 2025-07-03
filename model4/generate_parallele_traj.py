#!/usr/bin/env python3
import json, math, random, sys, argparse
from pathlib import Path
import multiprocessing as mp

import numpy as np
import pandas as pd
import torch
from tqdm import tqdm
from geo_utils import signed_distance_torch

# ── CONFIG ──
NUM_TRAJ = 1_000_000
MAX_LEN = 300
MIN_STEPS = 3
MARGIN_M = 100
CENTER_LAT, CENTER_LON = 51.0, 8.5
MIN_POINTS = 5

BASE = Path(__file__).resolve().parent
MODEL_PATH = BASE / "best_lstm.pt"
STATS_PATH = BASE / "dataset" / "stats.json"
TXT_DIR = BASE / "flights_txt"
OUT_DIR = BASE / "generated"
OUT_DIR.mkdir(parents=True, exist_ok=True)

DEVICE = torch.device("cuda" if torch.cuda.is_available() else "mps" if torch.backends.mps.is_available() else "cpu")
sys.path.append(str(BASE))
from model import FlightLSTM

# ── airports ──
AIRPORTS = {
    "EDDL": (51.2895, 6.7668),
    "EDDK": (50.8659, 7.1427),
    "EDLP": (51.6141, 8.6163),
}
AP_RADIUS_KM = 1.5
M_PER_DEG_LAT = 111_132.0

def m_per_deg_lon(lat_deg):
    return 111_320.0 * np.cos(np.radians(lat_deg))

def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371.0
    dlat, dlon = map(math.radians, [lat2 - lat1, lon2 - lon1])
    a = math.sin(dlat / 2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2)**2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

# ── normalization ──
feat_keys = ["dlat_m", "dlon_m", "dalt", "dtime", "gs_kt", "heading_sin", "heading_cos"]
with open(STATS_PATH) as f:
    stats = json.load(f)
mean = torch.tensor([stats[k]["mean"] for k in feat_keys], dtype=torch.float32)
std = torch.tensor([stats[k]["std"] for k in feat_keys], dtype=torch.float32)
def norm(x): return (x - mean) / std
def denorm(x): return x * std + mean

# ── preload TXT files ──
TXT_FILES = list(TXT_DIR.glob("*.txt"))
TXT_CONTENTS = [pd.read_csv(p) for p in TXT_FILES]
if not TXT_CONTENTS:
    print("No txt in", TXT_DIR)
    sys.exit(1)

def sample_seed(rng):
    df = rng.choice(TXT_CONTENTS)
    first = df.iloc[0]
    lat, lon, alt = float(first["lat"]), float(first["lon"]), float(first["alt_ft"])
    dy_m = (CENTER_LAT - lat) * M_PER_DEG_LAT
    dx_m = (CENTER_LON - lon) * m_per_deg_lon(lat)
    norm_v = math.hypot(dy_m, dx_m) + 1e-6
    dlat1 = dy_m / norm_v * 2000
    dlon1 = dx_m / norm_v * 2000
    heading_rad = math.atan2(dx_m, dy_m)

    seed_raw = torch.tensor([
        [0, 0, 0, 5, 400, math.sin(heading_rad), math.cos(heading_rad)],
        [dlat1, dlon1, 0, 5, 400, math.sin(heading_rad), math.cos(heading_rad)],
    ], dtype=torch.float32)
    return norm(seed_raw).unsqueeze(0), {"lat": lat, "lon": lon, "alt": alt}

def get_existing_ids():
    existing = []
    for f in OUT_DIR.glob("generated_flight_*.txt"):
        try:
            id_ = int(f.stem.split("_")[-1])
            existing.append(id_)
        except ValueError:
            continue
    return set(existing)

def generate_one(args):
    shared_counter, lock = args
    torch.set_grad_enabled(False)
    local_rng = random.Random()
    model = FlightLSTM(feat_dim=7).to(DEVICE)
    model.load_state_dict(torch.load(MODEL_PATH, map_location=DEVICE))
    model.eval()

    for _ in range(50):  # max essais
        try:
            seed_norm, start = sample_seed(local_rng)
            seed_norm = seed_norm.to(DEVICE)
            seq = [seed_norm]
            inp = seed_norm

            for step in range(MAX_LEN - 2):
                mu = model(inp)[:, -1:]
                next_step = mu + 0.05 * torch.randn_like(mu)
                seq.append(next_step)
                inp = torch.cat([inp, next_step], dim=1)

                denorm_all = denorm(torch.cat(seq, dim=1)[0].cpu())
                dlat_m = denorm_all[:, 0].numpy()
                dlon_m = denorm_all[:, 1].numpy()
                lat_deg = np.cumsum(dlat_m / M_PER_DEG_LAT) + start["lat"]
                lon_deg = start["lon"] + np.cumsum(dlon_m / m_per_deg_lon(lat_deg))
                cur_lat, cur_lon = lat_deg[-1], lon_deg[-1]

                if step + 2 >= MIN_STEPS:
                    dist = signed_distance_torch(
                        torch.tensor([[cur_lat]], dtype=torch.float32, device=DEVICE),
                        torch.tensor([[cur_lon]], dtype=torch.float32, device=DEVICE)
                    ).item()
                    if dist > MARGIN_M:
                        break
                    if any(haversine_km(cur_lat, cur_lon, a, o) < AP_RADIUS_KM for a, o in AIRPORTS.values()):
                        break

            denorm_seq = denorm(torch.cat(seq, dim=1).squeeze(0).cpu()).numpy()
            if len(denorm_seq) < MIN_POINTS:
                continue

            lat_out = lat_deg[:len(denorm_seq)]
            lon_out = lon_deg[:len(denorm_seq)]
            df = pd.DataFrame({
                "lat": lat_out,
                "lon": lon_out,
                "alt_ft": np.cumsum(denorm_seq[:, 2]) + start["alt"],
                "delta_t_s": denorm_seq[:, 3],
                "gs_kt": denorm_seq[:, 4],
                "heading_deg": (np.degrees(np.arctan2(denorm_seq[:, 5], denorm_seq[:, 6])) + 360) % 360,
            })

            with lock:
                idx = shared_counter.value
                shared_counter.value += 1

            out_path = OUT_DIR / f"generated_flight_{idx:06d}.txt"
            df.to_csv(out_path, index=False)
            return len(df)
        except:
            continue
    return 0

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--workers", type=int, default=3, help="Number of worker processes")
    args = parser.parse_args()

    print(f"Generation device: {DEVICE}")
    existing_ids = get_existing_ids()
    n_existing = len(existing_ids)

    if n_existing >= NUM_TRAJ:
        print(f"{n_existing} trajectories already exist Nothing to do")
        return

    start_id = max(existing_ids) + 1 if existing_ids else 0
    remaining = NUM_TRAJ - n_existing
    print(f"Found {n_existing} existing files Generating {remaining} more starting from ID {start_id}")

    with mp.get_context("spawn").Manager() as manager:
        counter = manager.Value('i', start_id)
        lock = manager.Lock()

        with mp.get_context("spawn").Pool(args.workers) as pool:
            args_list = [(counter, lock)] * remaining
            results = list(tqdm(pool.imap_unordered(generate_one, args_list), total=remaining))

    n_valid = sum(1 for r in results if r > 0)
    avg_pts = sum(results) / max(n_valid, 1)
    print(f"\nGenerated {n_valid} valid trajectories.")
    print(f"Average points per trajectory: {avg_pts:.1f}")

if __name__ == "__main__":
    mp.set_start_method("spawn", force=True)
    main()