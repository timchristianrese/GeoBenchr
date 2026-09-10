import os
import time
import pickle
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from multiprocessing import Pool, cpu_count
from datetime import datetime, timedelta

# =========================
# CONFIG
# =========================
INPUT_FILE = "staypoints.tsv"

SAMPLE_SIZE = 5_000_000
TOTAL_SYNTHETIC = 4_000_000_000
BATCH_SIZE = 5_000_000

OUTPUT_DIR = "synthetic_output"
CACHE_DIR = "cache"

SAMPLE_CACHE = os.path.join(CACHE_DIR, "sample.parquet")
DIST_CACHE = os.path.join(CACHE_DIR, "distributions.pkl")

N_WORKERS = max(1, cpu_count() - 1)

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(CACHE_DIR, exist_ok=True)

EXPECTED_COLS = ["UserId", "CheckinTime", "VenueId", "VenueType", "Latitude", "Longitude"]


# =========================
# VALIDATION
# =========================
def validate_df(df):
    missing = [c for c in EXPECTED_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns in cached sample: {missing}")

    return df[EXPECTED_COLS].copy()


# =========================
# STEP 1: RESERVOIR SAMPLING (FIXED + SAFE)
# =========================
def reservoir_sample_csv(file_path, sample_size):
    print("Sampling from CSV (safe mode)...")

    reservoir = []
    total_seen = 0

    for chunk in pd.read_csv(
        file_path,
        sep=",",
        chunksize=1_000_000,
        dtype={
            "UserId": np.int32,
            "VenueId": np.int32,
            "VenueType": "category",
            "Latitude": np.float32,
            "Longitude": np.float32,
            "CheckinTime": str
        }
    ):
        for row in chunk.itertuples(index=False, name=None):
            total_seen += 1

            if len(reservoir) < sample_size:
                reservoir.append(row)
            else:
                j = np.random.randint(0, total_seen)
                if j < sample_size:
                    reservoir[j] = row

        if total_seen % 10_000_000 == 0:
            print(f"[SAMPLING] {total_seen:,} rows processed")

    df = pd.DataFrame(reservoir, columns=EXPECTED_COLS)
    return df


def load_or_create_sample():
    if os.path.exists(SAMPLE_CACHE):
        print("Loading cached sample...")

        df = pq.read_table(SAMPLE_CACHE).to_pandas()

        # 🔥 critical fix: ensure schema integrity
        df = validate_df(df)

        return df

    df = reservoir_sample_csv(INPUT_FILE, SAMPLE_SIZE)

    print("Saving sample cache (no index)...")

    # 🔥 critical fix: NEVER include index
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, SAMPLE_CACHE, compression="snappy")

    return df


# =========================
# STEP 2: DISTRIBUTIONS (SAFE)
# =========================
def build_distributions(df):
    print("Building distributions...")

    df = validate_df(df)

    df["CheckinTime"] = pd.to_datetime(df["CheckinTime"], errors="coerce")
    df = df.dropna(subset=["CheckinTime"])

    df["hour"] = df["CheckinTime"].dt.hour

    user_counts = df["UserId"].value_counts(normalize=True)
    venue_counts = df["VenueId"].value_counts(normalize=True)
    hour_counts = df["hour"].value_counts(normalize=True)

    # ✅ normal venues only (exclude 0)
    venue_lookup = df[df["VenueId"] != 0] \
        .drop_duplicates("VenueId") \
        .set_index("VenueId")[["Latitude", "Longitude", "VenueType"]]

    # ✅ SPECIAL: collect ALL lat/lon for VenueId 0
    zero_df = df[df["VenueId"] == 0]

    dist = {
        "ids": user_counts.index.values,
        "user_probs": user_counts.values,
        "venue_ids": venue_counts.index.values,
        "venue_probs": venue_counts.values,
        "hours": hour_counts.index.values,
        "hour_probs": hour_counts.values,

        # normal venues
        "venue_lat": venue_lookup["Latitude"].to_dict(),
        "venue_lon": venue_lookup["Longitude"].to_dict(),
        "venue_type": venue_lookup["VenueType"].to_dict(),

        # 🔥 special pool for VenueId 0
        "zero_lat": zero_df["Latitude"].values,
        "zero_lon": zero_df["Longitude"].values,
        "zero_type": zero_df["VenueType"].values,
    }

    return dist


def load_or_create_distributions(df):
    if os.path.exists(DIST_CACHE):
        print("Loading cached distributions...")
        with open(DIST_CACHE, "rb") as f:
            return pickle.load(f)

    dist = build_distributions(df)

    with open(DIST_CACHE, "wb") as f:
        pickle.dump(dist, f)

    return dist


# =========================
# STEP 3: GENERATION
# =========================
def generate_batch(args):
    batch_id, size, dist = args

    np.random.seed()

    ids = np.random.choice(dist["ids"], size=size, p=dist["user_probs"])
    venue_ids = np.random.choice(dist["venue_ids"], size=size, p=dist["venue_probs"])

    lat = np.empty(size, dtype=np.float32)
    lon = np.empty(size, dtype=np.float32)
    vtype = np.empty(size, dtype=object)

    zero_mask = (venue_ids == 0)
    nonzero_mask = ~zero_mask


    lat[nonzero_mask] = [dist["venue_lat"][vid] for vid in venue_ids[nonzero_mask]]
    lon[nonzero_mask] = [dist["venue_lon"][vid] for vid in venue_ids[nonzero_mask]]
    vtype[nonzero_mask] = [dist["venue_type"][vid] for vid in venue_ids[nonzero_mask]]


    n_zero = zero_mask.sum()
    if n_zero > 0:
        idx = np.random.randint(0, len(dist["zero_lat"]), size=n_zero)

        lat[zero_mask] = dist["zero_lat"][idx]
        lon[zero_mask] = dist["zero_lon"][idx]
        vtype[zero_mask] = dist["zero_type"][idx]

    # July 2019 range
    start = np.datetime64("2019-07-01T00:00:00")
    end = np.datetime64("2019-08-01T00:00:00")

    # total seconds in range
    total_seconds = (end - start).astype("timedelta64[s]").astype(np.int64)

    # sample random offsets
    offsets = np.random.randint(0, total_seconds, size=size, dtype=np.int64)

    # build timestamps
    timestamps = start + offsets.astype("timedelta64[s]")

    df = pd.DataFrame({
        "UserId": ids.astype(np.int32),
        "CheckinTime": timestamps,
        "VenueId": venue_ids.astype(np.int32),
        "VenueType": vtype,
        "Latitude": lat,
        "Longitude": lon,
    })

    out_file = os.path.join(OUTPUT_DIR, f"part_{batch_id:05d}.parquet")

    pq.write_table(
        pa.Table.from_pandas(df, preserve_index=False),
        out_file,
        compression="snappy"
    )

    return batch_id


# =========================
# MAIN
# =========================
def main():
    df = load_or_create_sample()
    dist = load_or_create_distributions(df)

    print("Pipeline ready (cache safe + schema validated).")

    n_batches = (TOTAL_SYNTHETIC + BATCH_SIZE - 1) // BATCH_SIZE
    print(f"Total batches to generate: {n_batches:,}")

    # Build batch jobs
    jobs = []
    for i in range(n_batches):
        start = i * BATCH_SIZE
        end = min(start + BATCH_SIZE, TOTAL_SYNTHETIC)
        size = end - start

        jobs.append((i, size, dist))

    print(f"Starting generation using {N_WORKERS} workers...")

    start_time = time.time()

    # Parallel execution
    with Pool(N_WORKERS) as pool:
        for i, batch_id in enumerate(pool.imap_unordered(generate_batch, jobs), 1):
            if i % 10 == 0 or i == n_batches:
                elapsed = time.time() - start_time
                print(f"[PROGRESS] {i}/{n_batches} batches done | {elapsed:.1f}s elapsed")

    total_time = time.time() - start_time
    print(f"\n✅ Generation complete in {total_time/60:.2f} minutes")
    print(f"📁 Output written to: {OUTPUT_DIR}")
    
if __name__ == "__main__":
    main()