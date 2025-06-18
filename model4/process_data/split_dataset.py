#!/usr/bin/env python3
"""
Select a subset of trajectories for training:
"""

import shutil, random, math
from pathlib import Path

import numpy as np
import pandas as pd
from tqdm import tqdm

# ────────── SETTINGS ──────────
SRC_DIR          = Path("flights_txt")
DST_DIR          = Path("flights_txt_for_training")
N_STRAIGHT       = 6000     # number of straight flights
N_TURN           = 8000     # number of turning flights
THRESH_STRAIGHT  = 250.0     # total |Δ heading| under this = straight
MOVE_FILES       = False     # True = move files, False = copy files
SEED             = 42
# ------------------------------

random.seed(SEED)
DST_DIR.mkdir(parents=True, exist_ok=True)

def turn_score(path: Path) -> float:
    """Compute the total change in heading for one .txt file."""
    df = pd.read_csv(path)
    #hdg = df["heading_deg"].fillna(method="ffill").fillna(0).values
    hdg = df["heading_deg"].ffill().fillna(0) #no warning
    d_hdg = np.abs(np.diff(hdg))
    d_hdg = np.where(d_hdg > 180, 360 - d_hdg, d_hdg)
    return float(d_hdg.sum())

straight, turns = [], []
for f in tqdm(sorted(SRC_DIR.glob("*.txt")), desc="Scoring"):
    score = turn_score(f)
    (straight if score < THRESH_STRAIGHT else turns).append(f)

print(f"{len(straight)} straight flights, {len(turns)} turning flights (threshold: {THRESH_STRAIGHT})")

if len(straight) < N_STRAIGHT or len(turns) < N_TURN:
    raise RuntimeError("Not enough files in one category. Change N_STRAIGHT, N_TURN, or THRESH_STRAIGHT")

sel_straight = random.sample(straight, N_STRAIGHT)
sel_turns    = random.sample(turns,    N_TURN)
selected     = sel_straight + sel_turns

print(f"{len(selected)} files selected for transfer")

# Copy or move selected files
for src in tqdm(selected, desc="Transferring"):
    dst = DST_DIR / src.name
    if MOVE_FILES:
        shutil.move(src, dst)
    else:
        shutil.copy2(src, dst)

print("Output folder:", DST_DIR.resolve())
