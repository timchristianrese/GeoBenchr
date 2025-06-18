import sys, random
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[1]))

import torch
from torch.utils.data import DataLoader, random_split
from tqdm import tqdm

from process_data.dataset import FlightDataset, collate_pad
from model import FlightLSTM

# ------------------------- hyper‑paramètres -------------------------
BATCH        = 32
LR           = 1e-3
MAX_EPOCHS   = 50
PATIENCE     = 4           # early‑stop patience
VAL_RATIO    = 0.10
GRAD_CLIP    = 1.0
NOISE_STD    = 0.005       # input noise
TEACHER_P    = 0.9         # 90 % teacher‑forcing
# -------------------------------------------------------------------

device = torch.device("mps" if torch.backends.mps.is_available() else "cpu")
print("Device :", device)

FEAT_WEIGHTS = torch.tensor(
    [1.0, 1.0, 0.5, 0.2, 0.1, 1.0, 1.0],
    dtype=torch.float32,
    device=device
)

# ------------------------- dataset & split --------------------------
full_ds = FlightDataset()
n_val   = int(len(full_ds) * VAL_RATIO)
n_train = len(full_ds) - n_val
train_ds, val_ds = random_split(full_ds, [n_train, n_val],
                                generator=torch.Generator().manual_seed(42))

dl_train = DataLoader(train_ds, batch_size=BATCH, shuffle=True,
                      collate_fn=collate_pad, num_workers=0)
dl_val   = DataLoader(val_ds,   batch_size=BATCH, shuffle=False,
                      collate_fn=collate_pad, num_workers=0)

model = FlightLSTM(feat_dim=7).to(device)
optim = torch.optim.AdamW(model.parameters(), lr=LR, weight_decay=1e-5)
crit  = torch.nn.SmoothL1Loss(reduction="none")
sched = torch.optim.lr_scheduler.CosineAnnealingLR(
            optim, T_max=10, eta_min=2e-5
)

def masked_loss(pred, target, mask):
    """
    pred, target : (B, T, F)   mask : (B, T)
    """
    loss = crit(pred, target)
    loss = loss * FEAT_WEIGHTS
    loss = loss * mask.unsqueeze(-1)
    return loss.sum() / mask.sum()

@torch.no_grad()
def evaluate():
    model.eval()
    tot, msk = 0.0, 0
    for x, y, m in dl_val:
        x, y, m = x.to(device), y.to(device), m.to(device)
        pred = model(x)
        loss = crit(pred, y) * FEAT_WEIGHTS   # même pondération
        tot += (loss * m.unsqueeze(-1)).sum().item()
        msk += m.sum().item()
    return tot / msk

# ------------------------- training ----------------------------
best_val = float("inf")
epochs_no_improve = 0

for epoch in range(1, MAX_EPOCHS + 1):
    model.train()
    running, batches = 0.0, 0

    for x_in, y_out, m in tqdm(dl_train, desc=f"epoch {epoch}"):
        x_in, y_out, m = x_in.to(device), y_out.to(device), m.to(device)

        if NOISE_STD > 0:
            x_in = x_in + torch.randn_like(x_in) * NOISE_STD

        pred = model(x_in)

        # -------- scheduled sampling 10 % --------
        if random.random() > TEACHER_P:
            x_in_ss = x_in.clone()
            x_in_ss[:, 1:] = pred.detach()[:, :-1]
            pred = model(x_in_ss)

        loss = masked_loss(pred, y_out, m)

        optim.zero_grad()
        loss.backward()
        torch.nn.utils.clip_grad_norm_(model.parameters(), GRAD_CLIP)
        optim.step()

        running += loss.item()
        batches += 1

    train_loss = running / batches
    val_loss   = evaluate()
    sched.step()

    print(f"epoch {epoch:02d} | train={train_loss:.5f} | val={val_loss:.5f}")

    # -------- early stopping --------
    if val_loss < best_val - 1e-4:
        best_val = val_loss
        epochs_no_improve = 0
        torch.save(model.state_dict(), Path("best_lstm.pt"))
        print("-- best model saved")
    else:
        epochs_no_improve += 1
        if epochs_no_improve >= PATIENCE:
            print("Early stopping – validation n’améliore plus")
            break

print(f"Meilleur modèle : val_loss={best_val:.5f} - best_lstm.pt")