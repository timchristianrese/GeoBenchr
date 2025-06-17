import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[1]))

import torch, random
from torch.utils.data import DataLoader, random_split
from tqdm import tqdm

from process_data.dataset import FlightDataset, collate_pad
from model import FlightLSTM

# ------------------------- hyper‑paramètres -------------------------
BATCH        = 32
LR           = 1e-3
MAX_EPOCHS   = 50
PATIENCE     = 4          # epochs sans amélioration avant early‑stop
VAL_RATIO    = 0.10       # 10 % du jeu pour la validation
GRAD_CLIP    = 1.0
NOISE_STD    = 0.005      # bruit d’input (0 pour désactiver)
# -------------------------------------------------------------------

device = torch.device("mps" if torch.backends.mps.is_available() else "cpu")
print("Device :", device)

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

# ------------------------- modèle & optimiser ----------------------
model = FlightLSTM().to(device)
optim = torch.optim.AdamW(model.parameters(), lr=LR, weight_decay=1e-5)
crit  = torch.nn.SmoothL1Loss(reduction="none")
sched = torch.optim.lr_scheduler.ReduceLROnPlateau(
    optim, mode="min", factor=0.5, patience=2
)

# ------------------------- fonctions utilitaires -------------------
def masked_loss(pred, target, mask):
    loss = crit(pred, target)
    return (loss * mask.unsqueeze(-1)).sum() / mask.sum()

@torch.no_grad()
def evaluate():
    model.eval()
    tot, msk = 0.0, 0
    for x, y, m in dl_val:
        x, y, m = x.to(device), y.to(device), m.to(device)
        pred = model(x)
        tot += (crit(pred, y) * m.unsqueeze(-1)).sum().item()
        msk += m.sum().item()
    return tot / msk

# ------------------------- entraînement ----------------------------
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
        loss = masked_loss(pred, y_out, m)

        optim.zero_grad()
        loss.backward()
        torch.nn.utils.clip_grad_norm_(model.parameters(), GRAD_CLIP)
        optim.step()

        running += loss.item()
        batches += 1

    train_loss = running / batches
    val_loss   = evaluate()
    sched.step(val_loss)

    print(f"epoch {epoch:02d} | train={train_loss:.5f} | val={val_loss:.5f}")

    # --- early stopping ---
    if val_loss < best_val - 1e-4:
        best_val = val_loss
        epochs_no_improve = 0
        torch.save(model.state_dict(), Path("best_lstm.pt"))
        print("   ✔ best model saved")
    else:
        epochs_no_improve += 1
        if epochs_no_improve >= PATIENCE:
            print("Early stopping – validation loss n’améliore plus")
            break

print(f"Meilleur modèle : val_loss={best_val:.5f} → best_lstm.pt")