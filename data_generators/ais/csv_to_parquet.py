# /// script
# requires-python = ">=3.10"
# dependencies = [
#   "numpy~=2.2",
#   "polars~=1.35",
# ]
# ///

"""
Convert raw Danish AIS daily CSV files (aisdk-YYYY-MM-DD.csv.gz) into two
filtered Parquet files per month, mirroring the SpaceTime tables
`crossing_points` and `crossings`.

Applies the same filtering pipeline as import_ais.py:

    1. Sort deterministically by (MMSI, Timestamp, SOG, Latitude, Longitude).
    2. Dedup on (MMSI, Timestamp) keeping the last row -- collapses
       multi-receiver echoes that share a ship and instant.
    3. Per-ship trip detection (SOG > 1 knot starts a trip; 20-min stop or
       60-min time gap ends it). Non-trip rows get crossing_id = 0.
    4. Project (Longitude, Latitude) -> Mireo World-Point (x, y) integers.
    5. Bounding-box filter on (x, y) (roughly Danish waters).
    6. Final dedup on (crossing_id, t, x, y) for crossing_points.

Outputs (one pair per calendar month):

    <out_dir>/crossing_points/aisdk-YYYY-MM.parquet
        crossing_id : UInt64
        t           : UInt32  (Unix epoch seconds)
        x           : Int32   (Mireo World-Point)
        y           : Int32   (Mireo World-Point)
        latitude    : Float64 (WGS84, kept alongside x/y for GeoBenchr's own
                                Postgres/PostGIS/MobilityDB pipeline, which
                                builds geometry(Point,3857) from lat/lon
                                rather than the Mireo projection -- see
                                server/multinode/ansible/roles/postgis_ais/
                                tasks/process_parquet.yml)
        longitude   : Float64 (WGS84, see latitude above)

    <out_dir>/crossings/aisdk-YYYY-MM.parquet
        crossing_id : UInt64
        t0          : UInt32  (trip start, Unix epoch seconds)
        t1          : UInt32  (trip end,   Unix epoch seconds)
        MMSI        : Int64
        x0, y0      : Int32   (trip start WPT)
        x1, y1      : Int32   (trip end   WPT)

Usage:
    uv run csv_to_parquet.py -i /path/to/raw/ais -o /path/to/out
"""

from __future__ import annotations

import argparse
import math
import pathlib
from datetime import datetime

import numpy as np
import polars as pl


# ---------- Filter parameters ----------
SOG_THRESHOLD = 1.0          # knots
STOP_TIME_MINUTES = 20
MAX_TIME_GAP_MINUTES = 60

# ---------- Mireo World-Point projection ----------
_RMF_MERIDIAN_BIN = 0x08000000  # 2 ** 27
_RMF_MERIDIAN_LAT = 180.0

# Bounding box on the (x, y) projection: roughly Danish waters.
# Corresponds to lon ~1.34°..18.78°, lat ~52.37°..60.49°.
X_MIN, X_MAX = 1_000_000, 14_000_000
Y_MIN, Y_MAX = 46_000_000, 57_000_000


def _strip_csv_suffix(filename: str) -> str:
	"""'aisdk-2024-05-01.csv.gz' -> 'aisdk-2024-05-01'; also accepts plain '.csv'
	(the AIS archive's monthly zips at aisdata.ais.dk contain uncompressed
	aisdk-YYYY-MM-DD.csv, not .csv.gz)."""
	base = pathlib.PurePath(filename).name
	if base.lower().endswith(".csv.gz"):
		return base[: -len(".csv.gz")]
	return base[: -len(".csv")]


def date_suffix_from_name(filename: str) -> str:
	"""'aisdk-2024-05-01.csv.gz' -> '240501' (YYMMDD)."""
	base = _strip_csv_suffix(filename)
	date_str = "-".join(base.split("-")[1:])
	return datetime.strptime(date_str, "%Y-%m-%d").strftime("%y%m%d")


def year_month_from_name(filename: str) -> str:
	"""'aisdk-2024-05-01.csv.gz' -> '2024-05'."""
	base = _strip_csv_suffix(filename)
	parts = base.split("-")
	return f"{parts[1]}-{parts[2]}"


def assign_crossing_ids(
	mmsi_arr: np.ndarray, ts_arr: np.ndarray, sog_arr: np.ndarray, date_suffix: str
) -> np.ndarray:
	"""Per-ship trip state machine. Returns crossing_id per row (0 = no trip)."""
	n = len(mmsi_arr)
	out = np.zeros(n, dtype=np.int64)
	if n == 0:
		return out

	ts_int = ts_arr.astype("datetime64[ns]").astype(np.int64)
	gap_min = np.empty(n, dtype=np.float64)
	gap_min[0] = np.nan
	gap_min[1:] = (ts_int[1:] - ts_int[:-1]) / 6e10  # ns -> minutes

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
	"""Process one daily CSV and return (crossing_points_df, crossings_df)."""
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

	df = df.with_columns(pl.Series("crossing_id", crossing_ids)).with_columns(
		pl.col("crossing_id").cast(pl.UInt64),
		pl.col("Timestamp").dt.epoch("s").cast(pl.UInt32).alias("t"),
		pl.col("MMSI").cast(pl.Int64),
		(pl.col("Longitude") / _RMF_MERIDIAN_LAT * _RMF_MERIDIAN_BIN)
			.round().cast(pl.Int32, strict=False).alias("x"),
		(
			((pl.col("Latitude") * (math.pi / 180.0)) / 2 + (math.pi / 4)).tan().log()
			/ math.pi
			* _RMF_MERIDIAN_BIN
		).round().cast(pl.Int32, strict=False).alias("y"),
		# GeoBenchr addition: keep the original WGS84 lat/lon alongside the
		# Mireo x/y projection. Our own Postgres/PostGIS/MobilityDB pipeline
		# (process_parquet.yml) reads crossing_id/t/latitude/longitude and
		# builds geometry(Point,3857) directly from lat/lon -- inverting the
		# Mireo projection back to WGS84 would be an unnecessary
		# precision-losing round trip when the source values are already
		# sitting right here.
		pl.col("Latitude").alias("latitude"),
		pl.col("Longitude").alias("longitude"),
	).filter(
		pl.col("x").is_between(X_MIN, X_MAX)
		& pl.col("y").is_between(Y_MIN, Y_MAX)
	)

	# crossing_points: per-ping rows, no MMSI. Final dedup on (crossing_id, t, x, y)
	# -- explicit subset (rather than a bare .unique()) so adding latitude/longitude
	# above doesn't change which rows count as duplicates.
	df_points = (
		df.select("crossing_id", "t", "x", "y", "latitude", "longitude")
		.unique(subset=["crossing_id", "t", "x", "y"], keep="first")
		.sort("t")
	)

	# crossings: per-trip aggregate. Rows are still (MMSI, Timestamp)-sorted,
	# so first/last within each crossing_id group give earliest/latest values.
	df_crossings = (
		df.filter(pl.col("crossing_id") != 0)
		.group_by("crossing_id", maintain_order=True)
		.agg(
			pl.col("t").first().alias("t0"),
			pl.col("t").last().alias("t1"),
			pl.col("MMSI").first().alias("MMSI"),
			pl.col("x").first().alias("x0"),
			pl.col("y").first().alias("y0"),
			pl.col("x").last().alias("x1"),
			pl.col("y").last().alias("y1"),
		)
		.select("crossing_id", "t0", "t1", "MMSI", "x0", "y0", "x1", "y1")
		.sort("t0")
	)

	return df_points, df_crossings


def collect_files(in_paths: list[pathlib.Path]) -> list[pathlib.Path]:
	found: list[pathlib.Path] = []
	for p in in_paths:
		if p.is_file():
			if not (p.name.lower().endswith(".csv.gz") or p.name.lower().endswith(".csv")):
				raise RuntimeError(f"{p}: expected a .csv or .csv.gz file or a folder of them.")
			found.append(p)
		elif p.is_dir():
			found.extend(p.rglob("aisdk-*.csv.gz"))
			found.extend(p.rglob("aisdk-*.csv"))
		else:
			raise RuntimeError(f"{p}: not a file or directory.")
	uniq = {f.resolve(): f for f in found}
	return sorted(uniq.values(), key=lambda f: f.name)


def main() -> None:
	parser = argparse.ArgumentParser(
		description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
	)
	parser.add_argument(
		"-i", "--in-path", dest="in_paths", type=pathlib.Path, nargs="+", required=True,
		help="One or more aisdk-YYYY-MM-DD.csv.gz files and/or folders containing them.",
	)
	parser.add_argument(
		"-o", "--out-dir", type=pathlib.Path, required=True,
		help="Directory where monthly Parquet files will be written.",
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
		del points_pieces, crossings_pieces

		points_df.write_parquet(points_file, compression="snappy")
		print(f"   wrote {points_file.name} :: {points_df.height:,} rows", flush=True)

		crossings_df.write_parquet(crossings_file, compression="snappy")
		print(f"   wrote {crossings_file.name} :: {crossings_df.height:,} rows", flush=True)

		del points_df, crossings_df

	print("Done.")


if __name__ == "__main__":
	main()
