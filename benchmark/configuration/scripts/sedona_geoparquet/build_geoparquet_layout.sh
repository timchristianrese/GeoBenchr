#!/bin/bash
# Sedona: rewrite one HDFS point table as spatially partitioned GeoParquet (Track C, 2026-10-13), verify it and
# swap it in as hdfs:///geobenchr/<table>.geoparquet -- the path SedonaExecutor reads for tables listed in
# SEDONA_GEOPARQUET. Layout and reasoning: see GeoParquetLayout.java (same directory). In short: Sedona 1.8.1
# skips GeoParquet only per FILE (the bbox in each file's "geo" footer metadata), so every file must cover a
# small area: 64 Z-order cells per equal-row-count time slab, ~128 MB files, each file sorted by timestamp
# with 4 MB row groups and the timestamp as INT64 TIMESTAMP_MICROS (min/max statistics), so time filters
# skip row groups too. The old plain exports stored ride/crossing/geolife timestamps as INT96 without
# statistics: Spark could not prune them by time at all.
#
#   build_geoparquet_layout.sh <table>
# Steps: build at replication 1 into /geobenchr_gp/<table>.parquet (executors only on nst-2, nst-4, nst-5:
# nst-1 has no DataNode and nst-3 must keep disk headroom, and HDFS puts a writer's first replica on its own
# node), check every footer (TIMESTAMP_MICROS, min/max statistics, geo bbox), compare row counts with the
# plain table, then move it to /geobenchr/<table>.geoparquet, delete /geobenchr/<table>.parquet and raise
# the replication to 2. Stops before deleting anything if a check fails.
# Environment: EXECUTORS (default 40), EXCLUDE_NODES (default nst-1,nst-3), CHUNK_GB (plain GB per build
# job, default 60: bounds the shuffle's local disk use), KEEP_PLAIN=1 (do not delete/swap, just build+check).
set -euo pipefail
T=$1
EXECUTORS=${EXECUTORS:-40}
EXCLUDE_NODES=${EXCLUDE_NODES-nst-1,nst-3}
D=$HOME/trackC                      # GeoParquetLayout.class (javac -cp "<benchmark jar>:$HOME/bench_sedona/deps/*")
JAR=$HOME/realbench/benchmark_client-real52b.jar
H=/opt/hadoop/bin/hdfs
export JAVA_HOME=/opt/java/latest HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
LOG=$D/status.md
NEW=/geobenchr_gp/$T.parquet
log() { echo "$(date -Is) [$T] $*" >> $LOG; }

# tables already swapped to GeoParquet must be read from there (their plain export is gone)
gp_tables() {
  local l=""
  for t in crossing_points flight_points ride_points geolife_points; do
    $H dfs -test -e /geobenchr/$t.geoparquet && l="$l${l:+,}$t"
  done
  echo "$l"
}

run() {
  env SEDONA_GEOPARQUET=$(gp_tables) SUT_HOST=nst-1 SPARK_MASTER=yarn SPARK_HOME=/opt/spark HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop CHUNK_GB=${CHUNK_GB:-60} \
      SPARK_CONF="spark.sql.session.timeZone=UTC;spark.dynamicAllocation.enabled=false;spark.executor.instances=$EXECUTORS;spark.executor.cores=1;spark.executor.memory=2g;spark.sql.shuffle.partitions=400;spark.hadoop.dfs.replication=1;spark.sql.parquet.outputTimestampType=TIMESTAMP_MICROS;spark.sql.execution.rangeExchange.sampleSizePerPartition=1000${EXCLUDE_NODES:+;spark.yarn.exclude.nodes=$EXCLUDE_NODES}" \
    /opt/java/latest/bin/java -Xmx8g \
      --add-exports=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
      --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED \
      --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
      --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED \
      --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED \
      -cp "$D:$JAR:$HOME/bench_sedona/deps/*" GeoParquetLayout "$@"
}

log "build start ($NEW)"
$H dfs -mkdir -p /geobenchr_gp
run build $T hdfs://$NEW 64 128 4 > $D/build_$T.out 2>&1 || { log "build failed, see build_$T.out"; exit 1; }
log "built: $(grep 'done in' $D/build_$T.out)"
run verify hdfs://$NEW > $D/verify_$T.out 2>&1
V=$(grep '^VERIFY' $D/verify_$T.out) || { log "verify failed"; exit 1; }
log "$V"
case "$V" in *"ts statistics/geo metadata: 0") ;; *) log "footer check FAILED"; exit 1;; esac
run count $T hdfs://$NEW > $D/count_$T.out 2>&1
C=$(grep '^COUNT' $D/count_$T.out) || { log "count failed"; exit 1; }
log "$C"
case "$C" in *" OK") ;; *) log "row count MISMATCH, nothing swapped"; exit 1;; esac
[ "${KEEP_PLAIN:-0}" = 1 ] && { log "KEEP_PLAIN=1: not swapped"; exit 0; }
$H dfs -test -e /geobenchr/$T.geoparquet && { log "/geobenchr/$T.geoparquet exists, not swapped"; exit 1; }
$H dfs -mv $NEW /geobenchr/$T.geoparquet
$H dfs -rm -r -skipTrash /geobenchr/$T.parquet
$H dfs -setrep 2 /geobenchr/$T.geoparquet > /dev/null
log "swapped: /geobenchr/$T.geoparquet (replication 2 requested), /geobenchr/$T.parquet deleted"
