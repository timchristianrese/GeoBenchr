import os
import numpy as np
import pandas as pd

# Parameters
SEQ_LEN = 10
MAX_FILES = 10000

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INPUT_DIR = os.path.join(BASE_DIR, 'output_ais_txt')
OUTPUT_PATH = os.path.join(BASE_DIR, 'processed_data', 'raw_lstm_sequences.npz')
os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

X_all, y_all = [], []
files = [f for f in os.listdir(INPUT_DIR) if f.endswith('.txt')][:MAX_FILES]

for file in files:
    df = pd.read_csv(os.path.join(INPUT_DIR, file))
    if len(df) < SEQ_LEN + 1:
        continue
    try:
        data = df[['lat', 'lon', 'speed', 'course', 'depth']].values
        for i in range(len(data) - SEQ_LEN):
            X_all.append(data[i:i+SEQ_LEN])
            y_all.append(data[i+SEQ_LEN][:2])  # Predict next lat, lon
    except:
        continue

X_all = np.array(X_all)
y_all = np.array(y_all)
np.savez(OUTPUT_PATH, X=X_all, y=y_all)

print(f"Saved: {X_all.shape[0]} sequences")