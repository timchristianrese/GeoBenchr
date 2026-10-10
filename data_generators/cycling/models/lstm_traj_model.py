import torch
import torch.nn as nn

class LSTMTripPredictor(nn.Module):
    def __init__(self, vocab_size, embedding_dim=64, hidden_dim=128, feature_dim=3, lstm_layers=1):
        super().__init__()

        # Learnable embedding for each node index
        self.embedding = nn.Embedding(vocab_size, embedding_dim)
        # Total input dimension to the LSTM = embedding + extra features
        self.lstm_input_dim = embedding_dim + feature_dim

        # LSTM that takes combined embedding and features
        self.lstm = nn.LSTM(
            input_size=self.lstm_input_dim,
            hidden_size=hidden_dim,
            num_layers=lstm_layers,
            batch_first=True # input/output format: (batch, seq_len, features)
        )
        # Final linear layer to map LSTM output to vocabulary size (for prediction)
        self.output = nn.Linear(hidden_dim, vocab_size)

    # B : batch size, T : time steps
    def forward(self, x):
        """
        x: Tensor of shape (batch_size, seq_len, 1 + feature_dim)
           where x[..., 0] is node index and x[..., 1:] are features.
        """
        node_idxs = x[..., 0].long()          # Extract node indices → shape: (B, T)
        features = x[..., 1:].float()         # Extract numerical features → shape: (B, T, feature_dim)

        embeds = self.embedding(node_idxs)    # Convert node indices to embeddings → (B, T, embedding_dim)
        concat = torch.cat([embeds, features], dim=-1)  # Concatenate embeddings + features → (B, T, input_dim)

        lstm_out, _ = self.lstm(concat)       # Pass sequence into LSTM → (B, T, hidden_dim)
        last_output = lstm_out[:, -1, :]      # Get last time step output → (B, hidden_dim)
        logits = self.output(last_output)     # Predict next node index → (B, vocab_size)

        return logits

