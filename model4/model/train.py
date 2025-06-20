import sys
import random
import math
from pathlib import Path

import torch
from torch.utils.data import DataLoader, random_split
from tqdm import tqdm

sys.path.append(str(Path(__file__).resolve().parents[1]))

from process_data.dataset import FlightDataset, collate_pad
from model import FlightLSTM
from geo_utils import signed_distance_torch

# ── hyperparameters ──
BATCH        = 32
LR           = 1e-3
MAX_EPOCHS   = 50
PATIENCE     = 7
VAL_RATIO    = 0.10
GRAD_CLIP    = 1.0
NOISE_STD    = 0.005
TEACHER_P    = 0.9

# off road penalty
LAMBDA_OFF   = 0.2
OFF_MARGIN_M = 5000

device = torch.device("mps" if torch.backends.mps.is_available() else "cpu")
print("Device", device)

FEAT_WEIGHTS = torch.tensor(
    [1.0, 1.0, 0.5, 0.2, 0.1, 1.0, 1.0],
    dtype=torch.float32, device=device
)

# ── dataset ──
full_ds = FlightDataset()
n_val   = int(len(full_ds) * VAL_RATIO)
n_train = len(full_ds) - n_val
train_ds, val_ds = random_split(full_ds, [n_train, n_val],
                                generator=torch.Generator().manual_seed(42))

dl_train = DataLoader(train_ds, batch_size=BATCH, shuffle=True,
                      collate_fn=collate_pad, num_workers=0)
dl_val   = DataLoader(val_ds,   batch_size=BATCH, shuffle=False,
                      collate_fn=collate_pad, num_workers=0)

# ── model and optimizer ──
model = FlightLSTM(feat_dim=7).to(device)
optim = torch.optim.AdamW(model.parameters(), lr=LR, weight_decay=1e-5)
crit  = torch.nn.SmoothL1Loss(reduction="none")
sched = torch.optim.lr_scheduler.CosineAnnealingLR(
    optim, T_max=10, eta_min=2e-5
)

METERS_PER_DEG_LAT = 111_132.0
def meters_per_deg_lon(lat_deg):
    return 111_320.0 * torch.cos(lat_deg * math.pi / 180)

# ── loss function ──
def masked_loss(pred, target, mask, lat0, lon0):
    """
    pred, target : (B, T, 7)
    mask         : (B, T)
    lat0, lon0   : (B,)
    """
    # main weighted loss
    loss = crit(pred, target) * FEAT_WEIGHTS
    loss = (loss * mask.unsqueeze(-1)).sum() / mask.sum()

    # reconstruct absolute lat lon
    dlat_m = pred[..., 0]
    dlon_m = pred[..., 1]

    cum_dlat = torch.cumsum(dlat_m, dim=1)
    lat_deg  = lat0.unsqueeze(1).to(device) + cum_dlat / METERS_PER_DEG_LAT

    cum_dlon = torch.cumsum(dlon_m, dim=1)
    meters_per_deg = meters_per_deg_lon(lat_deg)
    lon_deg  = lon0.unsqueeze(1).to(device) + cum_dlon / meters_per_deg

    # off region penalty
    dist = signed_distance_torch(lat_deg, lon_deg, device)
    dist = dist.reshape(lat_deg.shape)

    off_pen = torch.relu(dist - OFF_MARGIN_M)
    off_pen = (off_pen * mask).sum() / mask.sum()

    return loss + LAMBDA_OFF * off_pen

@torch.no_grad()
def evaluate():
    model.eval()
    total_loss, total_pts = 0.0, 0
    for x, y, m, lat0, lon0 in dl_val:
        x, y, m = x.to(device), y.to(device), m.to(device)
        lat0, lon0 = lat0.to(device), lon0.to(device)
        pred = model(x)
        batch_loss = masked_loss(pred, y, m, lat0, lon0)
        total_loss += batch_loss.item() * m.sum().item()
        total_pts  += m.sum().item()
    return total_loss / total_pts

# ── training loop ──
best_val = float("inf")
epochs_no_improve = 0

for epoch in range(1, MAX_EPOCHS + 1):
    model.train()
    running_loss, total_batches = 0.0, 0

    for x_in, y_out, m, lat0, lon0 in tqdm(dl_train, desc=f"epoch {epoch}"):
        x_in, y_out, m = x_in.to(device), y_out.to(device), m.to(device)
        lat0, lon0 = lat0.to(device), lon0.to(device)

        if NOISE_STD > 0:
            x_in = x_in + torch.randn_like(x_in) * NOISE_STD

        pred = model(x_in)

        if random.random() > TEACHER_P:
            x_in_ss = x_in.clone()
            x_in_ss[:, 1:] = pred.detach()[:, :-1]
            pred = model(x_in_ss)

        loss = masked_loss(pred, y_out, m, lat0, lon0)

        optim.zero_grad()
        loss.backward()
        torch.nn.utils.clip_grad_norm_(model.parameters(), GRAD_CLIP)
        optim.step()

        running_loss += loss.item()
        total_batches += 1

    train_loss = running_loss / total_batches
    val_loss = evaluate()
    sched.step()

    print(f"epoch {epoch:02d}, train={train_loss:.5f}, val={val_loss:.5f}")

    if val_loss < best_val - 1e-4:
        best_val = val_loss
        epochs_no_improve = 0
        torch.save(model.state_dict(), Path("best_lstm.pt"))
        print("best model saved")
    else:
        epochs_no_improve += 1
        if epochs_no_improve >= PATIENCE:
            print("early stopping, no improvement on validation set")
            break

print(f"best model val_loss={best_val:.5f}, saved to best_lstm.pt")