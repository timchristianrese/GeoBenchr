import pandas as pd

# Load parquet file
input_file = "combined.parquet"
output_file = "output.parquet"

df = pd.read_parquet(input_file)

# Add 'id' column counting from 0
df.insert(0, "id", range(len(df)))

# Save back to parquet
df.to_parquet(output_file, index=False)

print(f"Added 'id' column and saved to {output_file}")