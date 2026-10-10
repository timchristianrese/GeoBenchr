import os
import random
from datetime import datetime, timedelta

import pyarrow as pa
import pyarrow.parquet as pq


# -----------------------------
# CONFIG
# -----------------------------
TOTAL_POINTS_TARGET = 2_700_000_000
ESTIMATED_POINTS_PER_FILE = 4_000_000

AVG_POINTS_PER_FLIGHT = 540
BATCH_FLIGHTS = 10_000

OUTPUT_DIR = "parquet_flights"

NRW_BOUNDS = {
    "lat_min": 50.3,
    "lat_max": 52.6,
    "lon_min": 5.5,
    "lon_max": 9.5,
}

START_DATE = datetime(2024, 1, 1)
END_DATE = datetime(2024, 12, 31, 23, 59, 59)


# -----------------------------
# PROGRESS DETECTION
# -----------------------------
def estimate_existing_progress():
    if not os.path.exists(OUTPUT_DIR):
        return 0, 0

    files = [f for f in os.listdir(OUTPUT_DIR) if f.endswith(".parquet")]
    file_count = len(files)

    estimated_points = file_count * ESTIMATED_POINTS_PER_FILE

    print(f"Detected {file_count} existing parquet files")
    print(f"Estimated existing points: {estimated_points:,}")

    return file_count, estimated_points


# -----------------------------
# HELPERS
# -----------------------------
def random_point():
    return (
        random.uniform(NRW_BOUNDS["lat_min"], NRW_BOUNDS["lat_max"]),
        random.uniform(NRW_BOUNDS["lon_min"], NRW_BOUNDS["lon_max"]),
    )


def interpolate(a, b, t):
    return a + (b - a) * t


def generate_altitude(t):
    if t < 0.2:
        return 1000 + (t / 0.2) * 9000
    elif t < 0.8:
        return 10000 + random.uniform(-500, 500)
    else:
        return 10000 - ((t - 0.8) / 0.2) * 9000


def generate_flight(id):
    lat1, lon1 = random_point()
    lat2, lon2 = random_point()

    n_points = random.randint(200, 600)

    start_time = START_DATE + timedelta(
        seconds=random.randint(0, int((END_DATE - START_DATE).total_seconds()))
    )

    duration = timedelta(minutes=random.randint(20, 180))

    for i in range(n_points):
        t = i / (n_points - 1)

        lat = interpolate(lat1, lat2, t)
        lon = interpolate(lon1, lon2, t)

        timestamp = start_time + duration * t
        altitude = generate_altitude(t)

        yield (
            id,
            lat,
            lon,
            timestamp,
            altitude,
        )


# -----------------------------
# MAIN
# -----------------------------
def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # detect progress
    existing_files, estimated_points = estimate_existing_progress()

    remaining_points = TOTAL_POINTS_TARGET - estimated_points

    if remaining_points <= 0:
        print("Target already reached. Nothing to do.")
        return

    print(f"Remaining points to generate: {remaining_points:,}")

    # estimate how many flights we still need
    estimated_remaining_flights = int(remaining_points / AVG_POINTS_PER_FLIGHT)

    print(f"Estimated remaining flights: {estimated_remaining_flights:,}")

    schema = pa.schema([
        ("id", pa.int64()),
        ("lat", pa.float64()),
        ("lon", pa.float64()),
        ("timestamp", pa.timestamp("s")),
        ("altitude", pa.float32()),
    ])

    file_index = existing_files
    id = existing_files * (ESTIMATED_POINTS_PER_FILE // AVG_POINTS_PER_FLIGHT)

    generated_points = 0

    while generated_points < remaining_points:
        batch_rows = []

        for _ in range(BATCH_FLIGHTS):
            for row in generate_flight(id):
                batch_rows.append(row)

            id += 1

        cols = list(zip(*batch_rows))

        table = pa.Table.from_arrays([
            pa.array(cols[0], type=pa.int64()),
            pa.array(cols[1], type=pa.float64()),
            pa.array(cols[2], type=pa.float64()),
            pa.array(cols[3], type=pa.timestamp("s")),
            pa.array(cols[4], type=pa.float32()),
        ], schema=schema)

        filename = os.path.join(
            OUTPUT_DIR, f"flights_{file_index:06d}.parquet"
        )

        pq.write_table(
            table,
            filename,
            compression="snappy",
            use_dictionary=True,
        )

        rows_written = len(batch_rows)
        generated_points += rows_written

        print(
            f"Wrote {filename} | rows: {rows_written:,} | "
            f"progress: {generated_points:,}/{remaining_points:,}"
        )

        file_index += 1


if __name__ == "__main__":
    main()