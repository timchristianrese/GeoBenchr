#!/bin/bash
# Fairness follow-up to the 52-repetition run (2026-10-11), driven from nst-inserter.
#
# 1. Preparation, nothing measured (all services up):
#    a. Sedona: crossing_points rewritten as GeoParquet like the other three point tables. Disk-neutral:
#       the plain Parquet goes to replication 1, the GeoParquet copy is written with replication 1 and
#       its row count checked, the plain Parquet moves to nst-1's local disk (~/hdfs_backup/, kept for
#       runs on plain Parquet), and the GeoParquet copy goes to replication 2 like the other tables.
#       If the build fails, Sedona reads all four tables as plain Parquet instead (same for every table).
#    b. DuckDB: crossing_points re-sorted by (day, Hilbert) -- the rule for every DuckDB point table is the
#       calendar unit whose buckets hold closest to 10 million rows (crossing_points was by month).
# 2. Measured, each system alone on the cluster, 52 repetitions per query as in the main run:
#    a. Sedona, all four scenarios, executors on all five nodes (90 x 1 core); client on nst-1.
#    b. DuckDB, AIS only (the one table re-sorted); client here, as in the main run.
# 3. Every service started again, logs and hardware samples collected, the result tables rebuilt with
#    the new Sedona and DuckDB/AIS logs replacing the old ones (kept in raw/superseded/), this host's
#    key removed from nst-1. Also if anything fails.
#   fair2.sh     log: ~/realbench/fair2.log; results: ~/realbench/analysis2/results/ (+ realbench_results2.tgz)
#   SKIP_RESORT=1 fair2.sh    the DuckDB re-sort is already done (second start, 2026-10-11)
#
# Replication (second start): the GeoParquet copy of crossing_points is 286 GB, 58 % more than the plain
# Parquet; at replication 2 it would push nst-4's disk past YARN's 95 % limit. All four GeoParquet tables
# therefore use replication 1 (the same for every table; frees disk overall).
set -u
R=$HOME/realbench
LOG=$R/fair2.log
N1=tim@10.1.0.11
NODES="nst-1 nst-2 nst-3 nst-4 nst-5"
JAR=$R/benchmark_client-real52b.jar
DEPS=$HOME/benchmark_client_deps
log() { echo "$(date -Is) $*" >> "$LOG"; }
n1() { ssh -o BatchMode=yes -o LogLevel=ERROR $N1 "$@"; }
HENV='export JAVA_HOME=/opt/java/latest HADOOP_HOME=/opt/hadoop HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop;'
JAVA_OPTS="-Dfile.encoding=UTF-8 --add-exports=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED
  --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED
  --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED
  --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED
  --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
GP_OK=0
DUCK_OK=0
SKIP_RESORT=${SKIP_RESORT:-0}
hsum() { n1 "$HENV /opt/hadoop/bin/hdfs dfs -du -s $1 | awk '{print \$1}'"; }        # bytes of all files
lsum() { n1 "find $1 -type f -printf '%s\n' 2>/dev/null | awk '{s+=\$1} END {printf \"%.0f\\n\", s}'"; }

finish() {
  log "restoring all services"
  n1 "~/realbench/svc.sh all" >> $LOG 2>&1
  n1 "for h in $NODES; do ssh -o LogLevel=ERROR \$h 'touch ~/hw_fair2/STOP'; done"
  touch $R/hw_fair2/STOP
  sleep 15
  # results: the main run's inputs, with the new Sedona and DuckDB/AIS logs in place of the old ones
  A=$R/analysis2/results/raw
  rm -rf $R/analysis2 && mkdir -p $R/analysis2 && cp -r $R/analysis/results $R/analysis2/ && cp $R/build_results.py $R/analysis2/
  rm -f $R/analysis2/results/*.csv $R/analysis2/results/tables.md && rm -rf $R/analysis2/results/plots
  mkdir -p $A/superseded $A/logs/fair2
  mkdir -p $R/out_fair2/Sedona && scp -q -o LogLevel=ERROR "$N1:realbench/out_fair2/Sedona/*" $R/out_fair2/Sedona/ 2>/dev/null
  if ls $R/out_fair2/Sedona/*query_results_*.log >/dev/null 2>&1; then
    mv $A/logs/real52/Sedona $A/superseded/Sedona_36_executors_3_nodes
    mkdir -p $A/logs/fair2/Sedona && cp $R/out_fair2/Sedona/*query_results_*.log $A/logs/fair2/Sedona/
  fi
  if ls $R/out_fair2/DuckDB/*query_results_*.log >/dev/null 2>&1; then
    mkdir -p $A/superseded/DuckDB_ais_month_buckets
    for f in $A/logs/real52/DuckDB/*query_results_*.log; do
      grep -q "\] \[ais\] " "$f" && mv "$f" $A/superseded/DuckDB_ais_month_buckets/
    done
    mkdir -p $A/logs/fair2/DuckDB && cp $R/out_fair2/DuckDB/*query_results_*.log $A/logs/fair2/DuckDB/
  fi
  mkdir -p $A/hardware_fair2
  n1 "rm -rf ~/realbench/hw_fair2_all && mkdir -p ~/realbench/hw_fair2_all && for h in $NODES; do scp -q -o LogLevel=ERROR \"\$h:hw_fair2/*.csv\" ~/realbench/hw_fair2_all/; done"
  scp -q -o LogLevel=ERROR "$N1:realbench/hw_fair2_all/*.csv" $A/hardware_fair2/
  cp $R/hw_fair2/*.csv $A/hardware_fair2/ 2>/dev/null
  # one sample file per host: the main run's samples followed by these
  for f in $A/hardware_fair2/*.csv; do b=$(basename $f); [ -f $A/hardware/$b ] && tail -n +2 $f >> $A/hardware/$b || cp $f $A/hardware/$b; done
  rm -rf $A/hardware_fair2
  cp $LOG $A/fair2.log
  n1 "sed -i '/geobenchr-benchmark\$/d' ~/.ssh/authorized_keys"
  (cd $R/analysis2 && RESULTS_DIR=$R/analysis2/results WARMUP_REPS=2 python3 build_results.py > build.out 2>&1)
  log "results built (rc=$?): $(tail -9 $R/analysis2/build.out | tr '\n' ' ')"
  tar -czf $R/realbench_results2.tgz -C $R/analysis2 results build_results.py
  log "ALL DONE (geoparquet=$GP_OK duckdb_resort=$DUCK_OK)"
}
trap finish EXIT
trap 'exit 1' TERM INT

log "start"
n1 "~/realbench/svc.sh all" >> $LOG 2>&1
# samplers still running from an earlier start are stopped first (they append to the same files)
n1 "for h in $NODES; do ssh -o LogLevel=ERROR \$h 'mkdir -p ~/hw_fair2 && touch ~/hw_fair2/STOP'; done"; mkdir -p $R/hw_fair2 && touch $R/hw_fair2/STOP; sleep 12
n1 "for h in $NODES; do ssh -o LogLevel=ERROR \$h 'rm -f ~/hw_fair2/STOP && (setsid nohup python3 ~/hw_sampler.py --out /home/tim/hw_fair2 --interval 5 --hours 72 >> ~/hw_fair2/sampler.out 2>&1 < /dev/null &)'; done"
rm -f $R/hw_fair2/STOP && (setsid nohup python3 $R/hw_sampler.py --out $R/hw_fair2 --interval 5 --hours 72 >> $R/hw_fair2/sampler.out 2>&1 < /dev/null &)

# ---- 1a. Sedona: crossing_points as GeoParquet ----
P=/geobenchr/crossing_points.parquet
B='~/hdfs_backup/crossing_points.parquet'
log "geoparquet: plain crossing_points.parquet to replication 1"
n1 "$HENV /opt/hadoop/bin/hdfs dfs -setrep -w 1 $P > /dev/null" >> $LOG 2>&1
log "geoparquet: build"
if n1 "EXECUTORS=90 EXCLUDE_NODES= REPLICATION=1 ~/sedona_geoparquet/build_geoparquet.sh crossing_points 'id, timestamp, lat, lon'" >> $LOG 2>&1; then
  a=$(n1 "clickhouse-client --port 9500 --query \"SELECT count() FROM hdfs('hdfs://nst-1:9000$P/*.parquet', 'Parquet')\"")
  b=$(n1 "clickhouse-client --port 9500 --query \"SELECT count() FROM hdfs('hdfs://nst-1:9000/geobenchr/crossing_points.geoparquet/*.parquet', 'Parquet')\"")
  log "geoparquet: rows plain=$a geoparquet=$b"
  if [ -n "$a" ] && [ "$a" = "$b" ]; then
    hb=$(hsum $P); lb=$(lsum $B)
    if [ "$hb" != "$lb" ]; then
      log "geoparquet: plain Parquet ($hb bytes) to nst-1:$B"
      n1 "$HENV mkdir -p ~/hdfs_backup && rm -rf $B && /opt/hadoop/bin/hdfs dfs -get $P ~/hdfs_backup/" >> $LOG 2>&1
      lb=$(lsum $B)
    fi
    log "geoparquet: backup check: HDFS $hb bytes, local copy $lb bytes"
    if [ -n "$hb" ] && [ "$hb" = "$lb" ]; then
      n1 "$HENV /opt/hadoop/bin/hdfs dfs -rm -r -skipTrash $P" >> $LOG 2>&1
      for t in flight_points ride_points geolife_points crossing_points; do
        n1 "$HENV /opt/hadoop/bin/hdfs dfs -setrep -w 1 /geobenchr/$t.geoparquet > /dev/null" >> $LOG 2>&1
      done
      n1 "grep -qx crossing_points ~/sedona_geoparquet/verified.txt || echo crossing_points >> ~/sedona_geoparquet/verified.txt"
      GP_OK=1
      log "geoparquet: done; all four point tables as GeoParquet, replication 1 ($(n1 'paste -sd, ~/sedona_geoparquet/verified.txt'))"
    else
      log "geoparquet: backup copy does not match -- plain Parquet kept on HDFS"
    fi
  fi
fi
if [ $GP_OK = 0 ]; then
  log "geoparquet: not used; plain crossing_points.parquet back to replication 2, Sedona reads all four tables as plain Parquet"
  n1 "$HENV /opt/hadoop/bin/hdfs dfs -setrep -w 2 $P > /dev/null; /opt/hadoop/bin/hdfs dfs -rm -r -f -skipTrash /geobenchr/crossing_points.geoparquet > /dev/null 2>&1" >> $LOG 2>&1
fi
n1 "df -h / | tail -1; for h in nst-2 nst-3 nst-4 nst-5; do ssh -o LogLevel=ERROR \$h 'echo \$(hostname) \$(df -h / | tail -1)'; done" >> $LOG 2>&1

# ---- 1b. DuckDB: crossing_points by (day, Hilbert) ----
if [ $SKIP_RESORT = 1 ]; then
  DUCK_OK=1; log "duckdb: re-sort already done (crossing_points by (day, Hilbert))"
elif log "duckdb: re-sort crossing_points by (day, Hilbert)"; n1 "cd ~/duckdb_reorder && ./duckdb_spatial_reorder_chunked.sh crossing_points crossing_points.check.sql day" >> $LOG 2>&1; then
  DUCK_OK=1; log "duckdb: done"
else
  log "duckdb: re-sort failed, crossing_points keeps its (month, Hilbert) order; DuckDB is not measured again"
fi

# ---- 2a. Sedona, alone, all five nodes ----
GPT=$([ $GP_OK = 1 ] && n1 'paste -sd, ~/sedona_geoparquet/verified.txt' || echo "")
if n1 "~/realbench/svc.sh only Sedona" >> $LOG 2>&1; then
  for i in $(seq 1 30); do
    nodes=$(n1 "curl -s http://nst-1:8088/ws/v1/cluster/metrics | python3 -c \"import json,sys; print(json.load(sys.stdin)['clusterMetrics']['activeNodes'])\"")
    [ "$nodes" = 5 ] && break; sleep 10
  done
  log "Sedona: $nodes active YARN nodes, GeoParquet tables: ${GPT:-none}"
  for SC in ais aviation cycling geolife; do
    log "Sedona $SC start"
    n1 "mkdir -p ~/realbench/out_fair2/Sedona && cd ~/realbench/out_fair2/Sedona && env SUT_HOST=nst-1 NUM_THREADS=1 SYSTEMS=Sedona SPARK_MASTER=yarn \
        SPARK_HOME=/opt/spark HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop SEDONA_GEOPARQUET='$GPT' \
        SPARK_CONF='spark.sql.session.timeZone=UTC;spark.dynamicAllocation.enabled=false;spark.executor.instances=90;spark.executor.cores=1;spark.executor.memory=2g' \
        /opt/java/latest/bin/java -Xmx6g $(echo $JAVA_OPTS) -cp ~/realbench/benchmark_client-real52b.jar:\$HOME/bench_sedona/deps/'*' \
        benchmark_client.BenchmarkClient $SC > ${SC}_stdout.log 2>&1"
    log "Sedona $SC end rc=$?"
  done
else
  log "Sedona: isolation failed, not measured"
fi

# ---- 2b. DuckDB AIS, alone ----
if [ $DUCK_OK = 1 ]; then
  if n1 "~/realbench/svc.sh only DuckDB" >> $LOG 2>&1; then
    mkdir -p $R/out_fair2/DuckDB && cd $R/out_fair2/DuckDB
    log "DuckDB ais start"
    env SUT_HOST=10.1.0.11 SUT_DB_PASSWORD=postgres NUM_THREADS=1 SYSTEMS=DuckDB \
        DUCKDB_CANCEL_CMD="ssh -o BatchMode=yes -o LogLevel=ERROR $N1 ~/realbench/svc.sh restart-duckdb >> $LOG 2>&1" \
        java $JAVA_OPTS -cp "$JAR:$DEPS/*" benchmark_client.BenchmarkClient ais > ais_stdout.log 2>&1
    log "DuckDB ais end rc=$?"
  else
    log "DuckDB: isolation failed, not measured"
  fi
fi
