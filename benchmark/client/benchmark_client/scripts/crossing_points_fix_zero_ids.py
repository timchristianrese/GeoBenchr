import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, monotonically_increasing_id
from sedona.spark import SedonaContext

# Follow-up fix to crossing_points_export_from_raw.py's output: that script correctly rebuilt
# crossing_points.parquet from the raw AIS files (deduplication verified, exact row-count match
# with Postgres: 5,984,837,218), but never applied the id=0 -> unique-high-id remapping Postgres's
# crossing_points has (1e12 + nextval('crossing_zero_id_seq'), applied Postgres-side earlier this
# session) -- confirmed live: the exported table had 2,167,247,164 rows at literal id=0 and none
# >= 1e12, the exact inverse of Postgres. That remapping was a deliberate fairness decision
# ("regarding benchmarking fairness, they would have to be included... having the singleton
# trajectories with unique (out of the normal ID range) IDs") so COUNT(DISTINCT id)-style queries
# don't collapse ~2.17B genuinely distinct "unmatched AIS" points into a single id=0 bucket.
#
# Rather than re-reading the ~150GB raw files again (re-staging them into HDFS, tight on
# per-node headroom right now: ~200-240GB free per node against a 146GB*3=438GB re-stage) and
# redoing the whole dedup pipeline, this reads the ALREADY-correct (deduplicated, row-count
# verified) live crossing_points.parquet directly and only fixes the id column -- much cheaper,
# no re-shuffle needed for anything but the id=0 partition itself.
HDFS_BASE = "hdfs:///geobenchr"
IN_PATH = f"{HDFS_BASE}/crossing_points.parquet"
OUT_PATH = f"{HDFS_BASE}/crossing_points_idfixed.parquet"

spark = SedonaContext.create(
    SparkSession.builder
    .appName("geobenchr-crossing-points-idfix")
    .master("yarn")
    .config("spark.submit.deployMode", "client")
    .getOrCreate()
)

t0 = time.time()
df = spark.read.parquet(IN_PATH)

zero_id = df.filter(col("id") == 0)
nonzero_id = df.filter(col("id") != 0)

# monotonically_increasing_id() gives a real per-row unique id without needing a global
# sort/sequence (an embarrassingly parallel per-partition counter) -- not the same *values*
# Postgres assigned, but cross-system id-value equality was never a requirement (each system's
# crossing_points is queried independently, not joined against another system's), only
# within-system uniqueness for correct COUNT(DISTINCT).
zero_id = zero_id.withColumn("id", (1000000000000 + monotonically_increasing_id()).cast("bigint"))
out = zero_id.unionByName(nonzero_id)

out.write.mode("overwrite").parquet(OUT_PATH)

elapsed = time.time() - t0
print(f"IDFIX_ELAPSED={elapsed:.1f}s", flush=True)
print("IDFIX COMPLETE", flush=True)
spark.stop()
