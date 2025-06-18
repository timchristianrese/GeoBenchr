import torch.nn as nn

class FlightLSTM(nn.Module):
    def __init__(self, feat_dim: int = 7,
                 hidden: int = 256,
                 layers: int = 3,
                 dropout: float = 0.3):
        super().__init__()
        self.lstm = nn.LSTM(
            input_size=feat_dim,
            hidden_size=hidden,
            num_layers=layers,
            batch_first=True,
            dropout=dropout if layers > 1 else 0.0,
        )
        self.dropout = nn.Dropout(dropout)
        self.head = nn.Linear(hidden, feat_dim)

    def forward(self, x):
        out, _ = self.lstm(x) # (B, T, hidden)
        out = self.dropout(out)
        return self.head(out) # (B, T, feat_dim)