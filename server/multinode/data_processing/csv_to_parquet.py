# /// script
# requires-python = ">=3.10"
# dependencies = [
#   "numpy~=2.2",
#   "polars~=1.35",
# ]
# ///

from __future__ import annotations

import argparse
import pathlib
from datetime import datetime

import numpy as np
import polars as pl


# ---------- Filter parameters ----------
SOG_THRESHOLD = 1.0          # knots
STOP_TIME_MINUTES = 20
MAX_TIME_GAP_MINUTES = 60

# ---------- Bounding box (lat/lon) ----------
# roughly Danish waters
X_MIN, X_MAX = 1.34, 18.78     # longitude
Y_MIN, Y_MAX = 52.37, 60.49    # latitude


def date_suffix_from_name(filename: str) -> str:
    base = pathlib.PurePath(filename).name.removesuffix(".csv.gz")
    date_str = "-".join(base.split("-")[1:])
    return datetime.strptime(date_str, "%Y-%m-%d").strftime("%y%m%d")


def year_month_from_name(filename: str) -> str:
    base = pathlib.PurePath(filename).name.removesuffix(".csv.gz")
    parts = base.split("-")
    return f"{parts[1]}-{parts[2]}"


def assign_ids(
    mmsi_arr: np.ndarray, ts_arr: np.ndarray, sog_arr: np.ndarray, date_suffix: str
) -> np.ndarray:
    n = len(mmsi_arr)
    out = np.zeros(n, dtype=np.int64)
    if n == 0:
        return out

    ts_int = ts_arr.astype("datetime64[ns]").astype(np.int64)
    gap_min = np.empty(n, dtype=np.float64)
    gap_min[0] = np.nan
    gap_min[1:] = (ts_int[1:] - ts_int[:-1]) / 6e10

    new_ship = np.empty(n, dtype=bool)
    new_ship[0] = True
    new_ship[1:] = mmsi_arr[1:] != mmsi_arr[:-1]
    gap_min[new_ship] = np.nan

    moving = sog_arr > SOG_THRESHOLD

    trip_counter = 0
    current_id = 0
    in_trip = False

    for i in range(n):
        if new_ship[i]:
            in_trip = False

        g = gap_min[i]

        if not np.isnan(g) and g > MAX_TIME_GAP_MINUTES:
            in_trip = False

        if not in_trip and moving[i]:
            trip_counter += 1
            current_id = int(f"{trip_counter}{date_suffix}")
            in_trip = True

        if in_trip and not moving[i]:
            if not np.isnan(g) and g >= STOP_TIME_MINUTES:
                in_trip = False

        if in_trip:
            out[i] = current_id

    return out


def process_file(path: pathlib.Path) -> tuple[pl.DataFrame, pl.DataFrame]:
    suffix = date_suffix_from_name(path.name)

    df = pl.read_csv(
        path,
        columns=["# Timestamp", "MMSI", "Latitude", "Longitude", "SOG"],
        schema_overrides={"MMSI": pl.Int64, "SOG": pl.Float64},
        try_parse_dates=False,
    )

    df = (
        df.with_columns(
            pl.col("# Timestamp")
            .str.strptime(pl.Datetime("ns"), format="%d/%m/%Y %H:%M:%S", strict=False)
            .alias("Timestamp")
        )
        .drop("# Timestamp")
        .sort(["MMSI", "Timestamp", "SOG", "Latitude", "Longitude"])
        .unique(subset=["MMSI", "Timestamp"], keep="last", maintain_order=True)
    )

    crossing_ids = assign_crossing_ids(
        df["MMSI"].to_numpy(),
        df["Timestamp"].to_numpy(),
        df["SOG"].to_numpy(),
        suffix,
    )

    df = (
        df.with_columns([
            pl.Series("crossing_id", crossing_ids).cast(pl.UInt64),
            pl.col("Timestamp").dt.epoch("s").cast(pl.UInt32).alias("t"),
            pl.col("MMSI").cast(pl.Int64),
            pl.col("Latitude").cast(pl.Float64),
            pl.col("Longitude").cast(pl.Float64),
        ])
        .filter(
            pl.col("Longitude").is_between(X_MIN, X_MAX)
            & pl.col("Latitude").is_between(Y_MIN, Y_MAX)
        )
    )

    # --- crossing_points: one row per AIS ping ---
    df_points = (
        df.select("crossing_id", "t", "Latitude", "Longitude")
        .unique(subset=["crossing_id", "t", "Latitude", "Longitude"])
        .sort("t")
        .rename({
            "Latitude": "latitude",
            "Longitude": "longitude",
        })
    )

    # --- crossings: start/end per trip ---
    df_crossings = (
        df.filter(pl.col("crossing_id") != 0)
        .group_by("crossing_id", maintain_order=True)
        .agg(
            pl.col("t").first().alias("t0"),
            pl.col("t").last().alias("t1"),
            pl.col("MMSI").first().alias("MMSI"),

            pl.col("Latitude").first().alias("lat0"),
            pl.col("Longitude").first().alias("lon0"),

            pl.col("Latitude").last().alias("lat1"),
            pl.col("Longitude").last().alias("lon1"),
        )
        .sort("t0")
    )

    return df_points, df_crossings


def collect_files(in_paths: list[pathlib.Path]) -> list[pathlib.Path]:
    found: list[pathlib.Path] = []

    for p in in_paths:
        if p.is_file():
            if not p.name.lower().endswith(".csv.gz"):
                raise RuntimeError(f"{p}: expected a .csv.gz file or a folder of them.")
            found.append(p)
        elif p.is_dir():
            found.extend(p.rglob("aisdk-*.csv.gz"))
        else:
            raise RuntimeError(f"{p}: not a file or directory.")

    uniq = {f.resolve(): f for f in found}
    return sorted(uniq.values(), key=lambda f: f.name)


def main() -> None:
    parser = argparse.ArgumentParser(description="AIS CSV -> monthly parquet converter")
    parser.add_argument(
        "-i", "--in-path",
        dest="in_paths",
        type=pathlib.Path,
        nargs="+",
        required=True,
    )
    parser.add_argument(
        "-o", "--out-dir",
        type=pathlib.Path,
        required=True,
    )

    args = parser.parse_args()

    files = collect_files(args.in_paths)
    if not files:
        raise RuntimeError(f"No aisdk-*.csv.gz found at {args.in_paths}.")

    points_dir = args.out_dir / "crossing_points"
    crossings_dir = args.out_dir / "crossings"
    points_dir.mkdir(parents=True, exist_ok=True)
    crossings_dir.mkdir(parents=True, exist_ok=True)

    by_month: dict[str, list[pathlib.Path]] = {}
    for f in files:
        by_month.setdefault(year_month_from_name(f.name), []).append(f)

    print(f"Found {len(files)} daily file(s) across {len(by_month)} month(s).")

    for month, month_files in by_month.items():
        points_file = points_dir / f"aisdk-{month}.parquet"
        crossings_file = crossings_dir / f"aisdk-{month}.parquet"

        print(f"== {month}: {len(month_files)} file(s)")

        points_pieces: list[pl.DataFrame] = []
        crossings_pieces: list[pl.DataFrame] = []

        for f in month_files:
            print(f"   {f.name} :: processing", flush=True)
            df_pts, df_crs = process_file(f)
            points_pieces.append(df_pts)
            crossings_pieces.append(df_crs)

        points_df = pl.concat(points_pieces, how="vertical").sort("t")
        crossings_df = pl.concat(crossings_pieces, how="vertical").sort("t0")

        points_df.write_parquet(points_file, compression="snappy")
        print(f"   wrote {points_file.name} :: {points_df.height:,} rows")

        crossings_df.write_parquet(crossings_file, compression="snappy")
        print(f"   wrote {crossings_file.name} :: {crossings_df.height:,} rows")

    print("Done.")


if __name__ == "__main__":
    main()