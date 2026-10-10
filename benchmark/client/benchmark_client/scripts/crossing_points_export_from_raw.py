import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, col, monotonically_increasing_id
from sedona.spark import SedonaContext

# Re-export of crossing_points.parquet on HDFS, built DIRECTLY from the raw monthly AIS source
# files (/var/lib/data/ais_raw/out/crossing_points/aisdk-*.parquet) instead of going back through
# Postgres via JDBC. The existing hdfs:///geobenchr/crossing_points.parquet is stale -- it predates
# the 2026-09-29/30 session's crossing_points reload (which itself was sourced from these exact
# raw files after DuckDB's own copy was found ~357M rows short) -- see
# benchmark_client/docs/smoke-test-2026-09-29.md's "Found: crossing_points.parquet in HDFS is
# stale" section for the full history.
#
# Why read the raw files directly rather than re-run sedona_export_parquet.py's normal
# Postgres-JDBC path: that path's own comment notes ride_points (50M rows, 256 JDBC partitions)
# took ~29 minutes; crossing_points is ~120x more rows (5,984,837,218) through the same
# per-row JDBC path, which would be well over a day. Reading the ~150GB of already-columnar raw
# parquet directly and writing straight to HDFS is a pure columnar transform with no per-row
# database round-trip -- and this project already trusts these exact files as the verified-correct
# source (they're what Postgres's own crossing_points was rebuilt from this session).
#
# Transform verified against a real row before writing this script (see the smoke-test doc):
# raw (crossing_id=593240101, t=1704067200, longitude=11.023567, latitude=55.374017) matches
# Postgres's (id=593240101, timestamp='2024-01-01 01:00:00', lon=11.023567, lat=55.374017,
# geom=ST_Transform(ST_SetSRID(ST_Point(lon,lat),4326),3857)) exactly.
#
# FIX: the raw files live at /var/lib/data/ais_raw/out/crossing_points/ on nst-1's LOCAL disk
# only (confirmed via `df`/`mount` -- a local /dev/md3 array, not NFS/shared) -- nst-2..5 have no
# such path at all. A distributed "--master yarn" read via file:// would fail for any task
# scheduled off nst-1. Staged into HDFS first instead (hdfs:///geobenchr_staging/
# crossing_points_raw/, ~146GB raw -> ~438GB at this cluster's 3x replication -- confirmed
# against `hdfs dfsadmin -report`'s 1.4TB DFS Remaining before doing this), matching how every
# other Sedona-facing table here already lives in HDFS. Delete that staging copy once this
# script's output is verified -- the local disk copy remains the permanent source of truth
# either way.
RAW_GLOB = "hdfs:///geobenchr_staging/crossing_points_raw/aisdk-*.parquet"
HDFS_BASE = "hdfs:///geobenchr"
OUT_PATH = f"{HDFS_BASE}/crossing_points_new.parquet"
EXPECTED_ROWS = 5984837218

LIMIT = None
if len(sys.argv) > 1 and sys.argv[1] == "--smoke-test":
    LIMIT = 1000

spark = SedonaContext.create(
    SparkSession.builder
    .appName("geobenchr-crossing-points-reexport")
    .master("yarn")
    .config("spark.submit.deployMode", "client")
    .getOrCreate()
)
# FIX: to_timestamp()/from_unixtime() interpret an epoch INT32 relative to this session
# timezone -- must match the Europe/Berlin wall-clock decode already verified against
# Postgres's own crossing_points.timestamp (a naive "timestamp without time zone" column).
spark.conf.set("spark.sql.session.timeZone", "Europe/Berlin")

t0 = time.time()
raw = spark.read.parquet(RAW_GLOB)
if LIMIT:
    raw = raw.limit(LIMIT)

df = (
    raw
    .withColumn("id", col("crossing_id").cast("bigint"))
    .withColumn("timestamp", expr("to_timestamp(from_unixtime(t))"))
    .withColumn("lat", col("latitude"))
    .withColumn("lon", col("longitude"))
    # Same WKB-normalize-on-write convention as sedona_export_parquet.py's other tables, so
    # SedonaExecutor.registerTables()'s existing ST_GeomFromWKB-on-read path needs no changes.
    .withColumn("geom", expr(
        "ST_AsBinary(ST_Transform(ST_SetSRID(ST_Point(longitude, latitude), 4326), 'EPSG:3857'))"
    ))
    .select("id", "timestamp", "lat", "lon", "geom")
)

# FIX: found via a full-run row-count check against Postgres's crossing_points (5,984,837,218)
# after the first successful export of this script came out 70,230 rows too high
# (5,984,907,448). Root-caused: Postgres's crossing_points has a real UNIQUE (id, timestamp)
# WHERE id <> 0 index (see \d crossing_points), which silently drops exact (id, timestamp)
# duplicates on insert (ON CONFLICT-style) -- most likely from a crossing whose id/timestamp
# gets emitted in two adjacent monthly raw files at a month boundary. Confirmed live: exactly
# 70,230 duplicate (id, timestamp) pairs exist among id != 0 rows in the raw read, matching the
# discrepancy exactly. id = 0 ("unmatched" placeholder crossings, ~36% of all rows) are
# deliberately excluded from Postgres's own unique index and must NOT be deduplicated the same
# way -- many genuinely distinct rows legitimately share id=0 at the same instant.
zero_id = df.filter(col("id") == 0)
nonzero_id = df.filter(col("id") != 0).dropDuplicates(["id", "timestamp"])

# FIX: found by comparing this table's id=0 handling against Postgres's crossing_points, which
# does NOT have literal id=0 rows at all -- earlier this session (crossings pipeline work),
# id=0 ("unmatched" AIS placeholder crossings, ~36% of all rows, confirmed: 2,167,247,164 here)
# was deliberately remapped to a unique id (1e12 + nextval('crossing_zero_id_seq')) on insert,
# a considered fairness decision ("regarding benchmarking fairness, they would have to be
# included... having the singleton trajectories with unique (out of the normal ID range) IDs")
# so COUNT(DISTINCT id)-style queries don't undercount by collapsing ~2.17B genuinely distinct
# points into a single id=0 bucket. That remapping is Postgres-only (a Postgres sequence) and
# was never applied here, since this script reads the raw files directly, bypassing Postgres
# entirely -- confirmed live: this table had 2,167,247,164 rows at literal id=0 and none >=
# 1e12, the exact inverse of Postgres. monotonically_increasing_id() gives a real per-row unique
# id without needing a global sort/sequence (unlike nextval(), it's an embarrassingly parallel
# per-partition counter) -- not the same *values* Postgres assigned, but cross-system id-value
# equality was never a requirement (each system's crossing_points is queried independently, not
# joined against another system's), only within-system uniqueness for correct COUNT(DISTINCT).
zero_id = zero_id.withColumn("id", (1000000000000 + monotonically_increasing_id()).cast("bigint"))
df = zero_id.unionByName(nonzero_id)

# FIX: no repartition()/coalesce() here on purpose -- sedona_export_parquet.py's own FIX comment
# documents a real incident where an extra repartition() shuffle on a multi-billion-row table
# (flight_points, 2.1B rows) caused a 4.5-hour run that never finished (executors repeatedly
# SIGTERM'd by YARN under shuffle memory pressure). That table's repartition was redundant with
# JDBC's own partitioned read; here there's no JDBC read to be redundant with in the first place --
# Spark's natural file/block-based partitioning from reading 19 ~5-10GB raw parquet files directly
# already gives real parallelism, so an explicit reshuffle to a specific partition count would be
# pure added risk for no benefit (Sedona's read side doesn't require any particular partition
# count or layout).
df.write.mode("overwrite").parquet(OUT_PATH)

elapsed = time.time() - t0
if LIMIT:
    print(f"SMOKE_TEST_ROWS_WRITTEN={df.count()} elapsed={elapsed:.1f}s", flush=True)
else:
    print(f"EXPORT_ELAPSED={elapsed:.1f}s", flush=True)
print("EXPORT COMPLETE", flush=True)
spark.stop()
