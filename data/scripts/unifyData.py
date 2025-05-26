import sys
import glob
import pandas as pd
from datetime import datetime
import os

CHUNK_SIZE = 50_000_000  # 50 million rows

def standardize_timestamp(ts):
    try:
        return pd.to_datetime(ts).isoformat()
    except Exception as e:
        print(f"Error parsing timestamp '{ts}': {e}")
        return None

def process_file(filepath_pattern, spatial, temporal, primary=None, output_name="output"):
    if spatial is None or temporal is None:
        print("Error: Both 'spatial' and 'temporal' parameters must be provided.")
        sys.exit(1)

    files = glob.glob(filepath_pattern+"*.csv")
    if not files:
        print("No files found matching the input pattern.")
        sys.exit(1)

    combined_df = pd.DataFrame()

    for filepath in files:
        print(f"Processing file: {filepath}")
        df = pd.read_csv(filepath)

        # Get column names based on indices
        lon_col = df.columns[spatial[0]]
        lat_col = df.columns[spatial[1]]
        temporal_col = df.columns[temporal[0]]
        primary_col = df.columns[primary] if primary is not None else None

        # Determine attribute columns
        excluded = {lon_col, lat_col, temporal_col}
        if primary_col:
            excluded.add(primary_col)
        attribute_cols = [col for col in df.columns if col not in excluded]

        # Apply transformation
        df["timestamp"] = df[temporal_col].apply(standardize_timestamp)
        df["wkt"] = df.apply(lambda row: f"POINT({row[lon_col]} {row[lat_col]})", axis=1)

        final_cols = ["timestamp", "wkt"]

        if primary_col:
            final_cols.append(primary_col)
        final_cols.extend(attribute_cols)

        transformed_df = df[final_cols]
        combined_df = pd.concat([combined_df, transformed_df], ignore_index=True)

    write_in_chunks(combined_df, output_name)

def write_in_chunks(df, base_output_name):
    total_rows = len(df)
    num_chunks = (total_rows // CHUNK_SIZE) + 1

    for i in range(num_chunks):
        start = i * CHUNK_SIZE
        end = min(start + CHUNK_SIZE, total_rows)
        if start >= end:
            break
        chunk_df = df.iloc[start:end]
        output_file = f"../processed/{base_output_name}/part{i+1}.csv"
        chunk_df.to_csv(output_file, index=False)
        print(f"Written chunk: {output_file} with {len(chunk_df)} rows")

def main():
    # Define parameters here
    process_file(
        filepath_pattern="../raw/ais/uni",  # Glob path to CSV files
        spatial=[2, 3],                 # Indexes for lon and latcd
        temporal=[0],                  # Index for timestamp
        primary=1,                     # Optional primary key index
        output_name="ais"          # Base name for output
    )
    process_file(
        filepath_pattern="../raw/aviation/NRW",  # Glob path to CSV files
        spatial=[6, 5],                 # Indexes for lon and lat
        temporal=[4],                  # Index for timestamp
        primary=None,                  # No primary key
        output_name="aviation"  # Base name for output
    )
    process_file(
        filepath_pattern="../raw/cycling/merged*",  # Glob path to CSV files
        spatial=[3, 2],                 # Indexes for lon and lat
        temporal=[4],                  # Index for timestamp
        primary=0,                     # Optional primary key index
        output_name="cycling"         # Base name for output
    )
    # #movebank
    # process_file(
    #     filepath_pattern="../raw/movebank/",  # Glob path to CSV files
    #     spatial=[],                 # Indexes for lon and lat
    #     temporal=[],                  # Index for timestamp
    #     primary=None,                  # No primary key
    #     output_name="movebank"       # Base name for output
    # )
    # #taxi
    # process_file(
    #     filepath_pattern="../raw/taxi/",  # Glob path to CSV files
    #     spatial=[],                 # Indexes for lon and lat
    #     temporal=[],                  # Index for timestamp
    #     primary=None,                  # No primary key
    #     output_name="taxi"       # Base name for output
    # )
    # #pedestrian
    # process_file(
    #     filepath_pattern="../raw/pedestrian/",  # Glob path to CSV files
    #     spatial=[],                 # Indexes for lon and lat
    #     temporal=[],                  # Index for timestamp
    #     primary=None,                  # No primary key
    #     output_name="pedestrian"       # Base name for output
    # )


if __name__ == "__main__":
    main()
