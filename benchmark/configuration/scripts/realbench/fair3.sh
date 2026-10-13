#!/bin/bash
# Fairness follow-up, third start (2026-10-12), driven from nst-inserter. Replaces fair2.sh's Sedona part,
# whose GeoParquet tables were sorted by geohash alone (time filters had to read everything: AIS queries
# with a time filter 2-2.8x slower than on time-ordered plain Parquet) and stored with HDFS replication 1
# (uneven load: one node at 81 % CPU, others at 12-44 %).
#
# 1. Preparation, nothing measured (all services up):
#    a. crossing_points' plain Parquet back onto HDFS from nst-1:~/hdfs_backup/ (replication 1; the source
#       of its GeoParquet build).
#    b. All four point tables rebuilt as GeoParquet sorted by (time bucket, geohash) -- the same bucket rule
#       as DuckDB's tables: the calendar unit whose buckets hold closest to 10 million rows (flight_points
#       and crossing_points by day, ride_points and geolife_points by hour) -- each checked row by row
#       count against its plain Parquet.
#    c. If all four are fine: crossing_points' plain Parquet off HDFS again (its backup stays on nst-1),
#       all four GeoParquet tables to replication 2 (as before fair2.sh). Otherwise Sedona reads all four
#       tables as plain Parquet.
# 2. Measured, each system alone on the cluster, 52 repetitions per query as in the main run:
#    a. Sedona, all four scenarios, 90 single-core executors on all five nodes; client on nst-1.
#    b. DuckDB, AIS only (crossing_points re-sorted by (day, Hilbert) on 2026-10-11); client here.
# 3. Every service started again, logs and hardware samples collected, the result tables rebuilt with
#    the new Sedona and DuckDB/AIS logs replacing the old ones (kept in raw/superseded/), this host's key
#    removed from nst-1. Also if anything fails.
#   fair3.sh     log: ~/realbench/fair3.log; results: ~/realbench/analysis3/results/ (+ realbench_results3.tgz)
set -u
R=$HOME/realbench
LOG=$R/fair3.log
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
hsum() { n1 "$HENV /opt/hadoop/bin/hdfs dfs -du -s $1 | awk '{print \$1}'"; }        # bytes of all files
lsum() { n1 "find $1 -type f -printf '%s\n' 2>/dev/null | awk '{s+=\$1} END {printf \"%.0f\\n\", s}'"; }

finish() {
  log "restoring all services"
  n1 "~/realbench/svc.sh all" >> $LOG 2>&1
  n1 "for h in $NODES; do ssh -o LogLevel=ERROR \$h 'touch ~/hw_fair3/STOP'; done"
  touch $R/hw_fair3/STOP
  sleep 15
  # results: the main run's inputs, with the new Sedona and DuckDB/AIS logs in place of the old ones
  A=$R/analysis3/results/raw
  rm -rf $R/analysis3 && mkdir -p $R/analysis3 && cp -r $R/analysis/results $R/analysis3/ && cp $R/build_results.py $R/analysis3/
  rm -f $R/analysis3/results/*.csv $R/analysis3/results/tables.md && rm -rf $R/analysis3/results/plots
  mkdir -p $A/superseded $A/logs/fair3
  mkdir -p $R/out_fair3/Sedona && scp -q -o LogLevel=ERROR "$N1:realbench/out_fair3/Sedona/*" $R/out_fair3/Sedona/ 2>/dev/null
  if ls $R/out_fair3/Sedona/*query_results_*.log >/dev/null 2>&1; then
    mv $A/logs/real52/Sedona $A/superseded/Sedona_36_executors_3_nodes
    mkdir -p $A/logs/fair3/Sedona && cp $R/out_fair3/Sedona/*query_results_*.log $A/logs/fair3/Sedona/
  fi
  if ls $R/out_fair3/DuckDB/*query_results_*.log >/dev/null 2>&1; then
    mkdir -p $A/superseded/DuckDB_ais_month_buckets
    for f in $A/logs/real52/DuckDB/*query_results_*.log; do
      grep -q "\] \[ais\] " "$f" && mv "$f" $A/superseded/DuckDB_ais_month_buckets/
    done
    mkdir -p $A/logs/fair3/DuckDB && cp $R/out_fair3/DuckDB/*query_results_*.log $A/logs/fair3/DuckDB/
  fi
  mkdir -p $A/hardware_fair3
  n1 "rm -rf ~/realbench/hw_fair3_all && mkdir -p ~/realbench/hw_fair3_all && for h in $NODES; do scp -q -o LogLevel=ERROR \"\$h:hw_fair3/*.csv\" ~/realbench/hw_fair3_all/; done"
  scp -q -o LogLevel=ERROR "$N1:realbench/hw_fair3_all/*.csv" $A/hardware_fair3/
  cp $R/hw_fair3/*.csv $A/hardware_fair3/ 2>/dev/null
  # one sample file per host: the main run's samples followed by these
  for f in $A/hardware_fair3/*.csv; do b=$(basename $f); [ -f $A/hardware/$b ] && tail -n +2 $f >> $A/hardware/$b || cp $f $A/hardware/$b; done
  rm -rf $A/hardware_fair3
  cp $LOG $A/fair3.log
  n1 "sed -i '/geobenchr-benchmark\$/d' ~/.ssh/authorized_keys"
  (cd $R/analysis3 && RESULTS_DIR=$R/analysis3/results WARMUP_REPS=2 python3 build_results.py > build.out 2>&1)
  log "results built (rc=$?): $(tail -9 $R/analysis3/build.out | tr '\n' ' ')"
  tar -czf $R/realbench_results3.tgz -C $R/analysis3 results build_results.py
  log "ALL DONE (geoparquet=$GP_OK duckdb_resort=$DUCK_OK)"
}
trap finish EXIT
trap 'exit 1' TERM INT

log "start"
n1 "~/realbench/svc.sh all" >> $LOG 2>&1
# samplers still running from an earlier start are stopped first (they append to the same files)
n1 "for h in $NODES; do ssh -o LogLevel=ERROR \$h 'mkdir -p ~/hw_fair3 && touch ~/hw_fair3/STOP'; done"; mkdir -p $R/hw_fair3 && touch $R/hw_fair3/STOP; sleep 12
n1 "for h in $NODES; do ssh -o LogLevel=ERROR \$h 'rm -f ~/hw_fair3/STOP && (setsid nohup python3 ~/hw_sampler.py --out /home/tim/hw_fair3 --interval 5 --hours 72 >> ~/hw_fair3/sampler.out 2>&1 < /dev/null &)'; done"
rm -f $R/hw_fair3/STOP && (setsid nohup python3 $R/hw_sampler.py --out $R/hw_fair3 --interval 5 --hours 72 >> $R/hw_fair3/sampler.out 2>&1 < /dev/null &)

# ---- 1a. crossing_points' plain Parquet back onto HDFS ----
P=/geobenchr/crossing_points.parquet
B='~/hdfs_backup/crossing_points.parquet'
if [ "$(hsum $P 2>/dev/null)" != "$(lsum $B)" ]; then
  log "geoparquet: crossing_points plain Parquet back onto HDFS (replication 1)"
  n1 "$HENV /opt/hadoop/bin/hdfs dfs -rm -r -f -skipTrash $P > /dev/null 2>&1; /opt/hadoop/bin/hdfs dfs -D dfs.replication=1 -put $B /geobenchr/" >> $LOG 2>&1
fi
hb=$(hsum $P); lb=$(lsum $B)
log "geoparquet: crossing_points plain Parquet on HDFS $hb bytes, backup $lb bytes"

# ---- 1b. all four point tables as GeoParquet sorted by (time bucket, geohash) ----
ALL=1
if [ -n "$hb" ] && [ "$hb" = "$lb" ]; then
  for spec in "flight_points|day|id, timestamp, altitude" "ride_points|hour|id, rider_id, timestamp, line_geom" \
              "geolife_points|hour|id, timestamp, venue_id, venue_type, lat, lon" "crossing_points|day|id, timestamp, lat, lon"; do
    t=${spec%%|*}; rest=${spec#*|}; bucket=${rest%%|*}; cols=${rest#*|}
    log "geoparquet: build $t sorted by ($bucket, geohash)"
    if n1 "BUCKET=$bucket EXECUTORS=90 EXCLUDE_NODES= REPLICATION=1 ~/sedona_geoparquet/build_geoparquet.sh $t '$cols'" >> $LOG 2>&1; then
      a=$(n1 "clickhouse-client --port 9500 --query \"SELECT count() FROM hdfs('hdfs://nst-1:9000/geobenchr/$t.parquet/*.parquet', 'Parquet')\"")
      b=$(n1 "clickhouse-client --port 9500 --query \"SELECT count() FROM hdfs('hdfs://nst-1:9000/geobenchr/$t.geoparquet/*.parquet', 'Parquet')\"")
      log "geoparquet: $t rows plain=$a geoparquet=$b"
      { [ -n "$a" ] && [ "$a" = "$b" ]; } || ALL=0
    else
      log "geoparquet: $t build failed"; ALL=0
    fi
  done
else
  ALL=0; log "geoparquet: crossing_points plain Parquet not restored, no rebuild"
fi

# ---- 1c. replication ----
if [ $ALL = 1 ]; then
  n1 "$HENV /opt/hadoop/bin/hdfs dfs -rm -r -skipTrash $P" >> $LOG 2>&1
  for t in flight_points ride_points geolife_points crossing_points; do
    n1 "$HENV /opt/hadoop/bin/hdfs dfs -setrep -w 2 /geobenchr/$t.geoparquet > /dev/null" >> $LOG 2>&1
  done
  GP_OK=1
  log "geoparquet: done; all four point tables as GeoParquet sorted by (time bucket, geohash), replication 2"
else
  n1 "$HENV /opt/hadoop/bin/hdfs dfs -setrep -w 2 $P > /dev/null 2>&1" >> $LOG 2>&1
  log "geoparquet: not used; Sedona reads all four point tables as plain Parquet"
fi
n1 "$HENV /opt/hadoop/bin/hdfs dfs -du -s -h '/geobenchr/*_points.*'; df -h / | tail -1; for h in nst-2 nst-3 nst-4 nst-5; do ssh -o LogLevel=ERROR \$h 'echo \$(hostname) \$(df -h / | tail -1)'; done" >> $LOG 2>&1
DUCK_OK=1

# ---- 2a. Sedona, alone, all five nodes ----
GPT=$([ $GP_OK = 1 ] && echo "flight_points,ride_points,geolife_points,crossing_points" || echo "")
if n1 "~/realbench/svc.sh only Sedona" >> $LOG 2>&1; then
  for i in $(seq 1 30); do
    nodes=$(n1 "curl -s http://nst-1:8088/ws/v1/cluster/metrics | python3 -c \"import json,sys; print(json.load(sys.stdin)['clusterMetrics']['activeNodes'])\"")
    [ "$nodes" = 5 ] && break; sleep 10
  done
  log "Sedona: $nodes active YARN nodes, GeoParquet tables: ${GPT:-none}"
  for SC in ais aviation cycling geolife; do
    log "Sedona $SC start"
    n1 "mkdir -p ~/realbench/out_fair3/Sedona && cd ~/realbench/out_fair3/Sedona && env SUT_HOST=nst-1 NUM_THREADS=1 SYSTEMS=Sedona SPARK_MASTER=yarn \
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
    mkdir -p $R/out_fair3/DuckDB && cd $R/out_fair3/DuckDB
    log "DuckDB ais start"
    env SUT_HOST=10.1.0.11 SUT_DB_PASSWORD=postgres NUM_THREADS=1 SYSTEMS=DuckDB \
        DUCKDB_CANCEL_CMD="ssh -o BatchMode=yes -o LogLevel=ERROR $N1 ~/realbench/svc.sh restart-duckdb >> $LOG 2>&1" \
        java $JAVA_OPTS -cp "$JAR:$DEPS/*" benchmark_client.BenchmarkClient ais > ais_stdout.log 2>&1
    log "DuckDB ais end rc=$?"
  else
    log "DuckDB: isolation failed, not measured"
  fi
fi
