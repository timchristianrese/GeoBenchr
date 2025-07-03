import os
import json
import torch
from torch.utils.data import DataLoader
from datasets.trip_dataset import TripSequenceDataset
from models.lstm_traj_model import LSTMTripPredictor

# Config for the training
seq_len = 10
batch_size = 64
embedding_dim = 64
hidden_dim = 128
feature_dim = 3 # [x, y, degree] or similar
num_epochs = 10
lr = 1e-3

base_dir = os.path.dirname(os.path.abspath(__file__))
data_path = os.path.join(base_dir, "data", "training", "human_sequences.jsonl")
vocab_path = os.path.join(base_dir, "data", "training", "node_vocab.json")
model_path = os.path.join(base_dir, "models", "trained_lstm_model.pt")

device = 'cuda' if torch.cuda.is_available() else 'cpu'

# ====================
# Data Preparation
# ====================

def collate_fn(batch): # Each batch is a tuple of (input_sequence, target_node_id)
    x_batch, y_batch = zip(*batch)
    x_tensor = torch.tensor(x_batch, dtype=torch.float32)
    y_tensor = torch.tensor(y_batch, dtype=torch.long)
    return x_tensor, y_tensor


dataset = TripSequenceDataset(data_path, seq_len=seq_len)
dataloader = DataLoader(dataset, batch_size=batch_size, shuffle=True, collate_fn=collate_fn)

# Load node vocabulary
with open(vocab_path) as f:
    vocab = json.load(f)
node_to_index = vocab['node_to_index']
vocab_size = len(node_to_index)


# ====================
# Model Setup
# ====================

model = LSTMTripPredictor(vocab_size, embedding_dim, hidden_dim, feature_dim=feature_dim).to(device)
optimizer = torch.optim.Adam(model.parameters(), lr=lr)
criterion = torch.nn.CrossEntropyLoss()

# ====================
# Training Loop
# ====================

print("Training LSTM model on bike route sequences")
for epoch in range(num_epochs):
    model.train()
    total_loss = 0

    for x_batch, y_batch in dataloader:
        # Convertit list to 3d tensor : (batch_size, seq_len, 4)
        x_tensor = torch.tensor(x_batch, dtype=torch.float32).to(device)
        y_tensor = torch.tensor(y_batch, dtype=torch.long).to(device)

        logits = model(x_tensor)
        loss = criterion(logits, y_tensor)

        optimizer.zero_grad()
        loss.backward()
        optimizer.step()

        total_loss += loss.item()

    avg_loss = total_loss / len(dataloader)
    print(f"Epoch {epoch+1}/{num_epochs} | Loss: {avg_loss:.4f}")


# Save modael
os.makedirs(os.path.dirname(model_path), exist_ok=True)
torch.save(model.state_dict(), model_path)
print(f"Model saved : {model_path}")
