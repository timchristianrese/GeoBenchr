#!/bin/bash
# Fairness follow-up, fourth start (2026-10-13), driven from nst-inserter: Sedona on plain Parquet.
#
# Sedona did not skip anything on our GeoParquet tables -- neither by time nor by bounding box: AIS queries
# with a time filter took as long as spatial ones and scaled with the bytes read (fair2.sh, fair3.sh), while
# on the time-ordered plain Parquet a time filter skips most row groups. So Sedona reads all four point
# tables as plain Parquet, the same for every table.
#
# 1. Preparation, nothing measured (all services up):
#    a. the GeoParquet copies deleted from HDFS (they can be rebuilt with build_geoparquet.sh);
#    b. crossing_points' plain Parquet back onto HDFS from nst-1:~/hdfs_backup/, replication 2 like the other
#       three plain Parquet tables, checked byte for byte.
# 2. Measured, each system alone on the cluster, 52 repetitions per query as in the main run:
#    a. Sedona, all four scenarios, 90 single-core executors on all five nodes; client on nst-1. Not run if
#       fewer than five YARN nodes are healthy, and stopped before a scenario if one has dropped out.
#    b. DuckDB, AIS only (crossing_points re-sorted by (day, Hilbert) on 2026-10-11); client here.
# 3. Every service started again, logs and hardware samples collected, the result tables rebuilt with the
#    new Sedona and DuckDB/AIS logs replacing the old ones (kept in raw/superseded/), this host's key
#    removed from nst-1. Also if anything fails.
#   fair4.sh     log: ~/realbench/fair4.log; results: ~/realbench/analysis4/results/ (+ realbench_results4.tgz)
set -u
R=$HOME/realbench
LOG=$R/fair4.log
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
  n1 "for h in $NODES; do ssh -o LogLevel=ERROR \$h 'touch ~/hw_fair4/STOP'; done"
  touch $R/hw_fair4/STOP
  sleep 15
  # results: the main run's inputs, with the new Sedona and DuckDB/AIS logs in place of the old ones
  A=$R/analysis4/results/raw
  rm -rf $R/analysis4 && mkdir -p $R/analysis4 && cp -r $R/analysis/results $R/analysis4/ && cp $R/build_results.py $R/analysis4/
  rm -f $R/analysis4/results/*.csv $R/analysis4/results/tables.md && rm -rf $R/analysis4/results/plots
  mkdir -p $A/superseded $A/logs/fair4
  mkdir -p $R/out_fair4/Sedona && scp -q -o LogLevel=ERROR "$N1:realbench/out_fair4/Sedona/*" $R/out_fair4/Sedona/ 2>/dev/null
  if ls $R/out_fair4/Sedona/*query_results_*.log >/dev/null 2>&1; then
    mv $A/logs/real52/Sedona $A/superseded/Sedona_36_executors_3_nodes
    mkdir -p $A/logs/fair4/Sedona && cp $R/out_fair4/Sedona/*query_results_*.log $A/logs/fair4/Sedona/
  fi
  if ls $R/out_fair4/DuckDB/*query_results_*.log >/dev/null 2>&1; then
    mkdir -p $A/superseded/DuckDB_ais_month_buckets
    for f in $A/logs/real52/DuckDB/*query_results_*.log; do
      grep -q "\] \[ais\] " "$f" && mv "$f" $A/superseded/DuckDB_ais_month_buckets/
    done
    mkdir -p $A/logs/fair4/DuckDB && cp $R/out_fair4/DuckDB/*query_results_*.log $A/logs/fair4/DuckDB/
  fi
  mkdir -p $A/hardware_fair4
  n1 "rm -rf ~/realbench/hw_fair4_all && mkdir -p ~/realbench/hw_fair4_all && for h in $NODES; do scp -q -o LogLevel=ERROR \"\$h:hw_fair4/*.csv\" ~/realbench/hw_fair4_all/; done"
  scp -q -o LogLevel=ERROR "$N1:realbench/hw_fair4_all/*.csv" $A/hardware_fair4/
  cp $R/hw_fair4/*.csv $A/hardware_fair4/ 2>/dev/null
  # one sample file per host: the main run's samples followed by these
  for f in $A/hardware_fair4/*.csv; do b=$(basename $f); [ -f $A/hardware/$b ] && tail -n +2 $f >> $A/hardware/$b || cp $f $A/hardware/$b; done
  rm -rf $A/hardware_fair4
  cp $LOG $A/fair4.log
  n1 "sed -i '/geobenchr-benchmark\$/d' ~/.ssh/authorized_keys"
  (cd $R/analysis4 && RESULTS_DIR=$R/analysis4/results WARMUP_REPS=2 python3 build_results.py > build.out 2>&1)
  log "results built (rc=$?): $(tail -9 $R/analysis4/build.out | tr '\n' ' ')"
  tar -czf $R/realbench_results4.tgz -C $R/analysis4 results build_results.py
  log "ALL DONE (plain_parquet=$PLAIN_OK)"
}
trap finish EXIT
trap 'exit 1' TERM INT

log "start"
n1 "~/realbench/svc.sh all" >> $LOG 2>&1
# samplers still running from an earlier start are stopped first (they append to the same files)
n1 "for h in $NODES; do ssh -o LogLevel=ERROR \$h 'mkdir -p ~/hw_fair4 && touch ~/hw_fair4/STOP'; done"; mkdir -p $R/hw_fair4 && touch $R/hw_fair4/STOP; sleep 12
n1 "for h in $NODES; do ssh -o LogLevel=ERROR \$h 'rm -f ~/hw_fair4/STOP && (setsid nohup python3 ~/hw_sampler.py --out /home/tim/hw_fair4 --interval 5 --hours 72 >> ~/hw_fair4/sampler.out 2>&1 < /dev/null &)'; done"
rm -f $R/hw_fair4/STOP && (setsid nohup python3 $R/hw_sampler.py --out $R/hw_fair4 --interval 5 --hours 72 >> $R/hw_fair4/sampler.out 2>&1 < /dev/null &)

# ---- 1a. GeoParquet copies off HDFS ----
for t in flight_points ride_points geolife_points crossing_points; do
  n1 "$HENV /opt/hadoop/bin/hdfs dfs -rm -r -f -skipTrash /geobenchr/$t.geoparquet" >> $LOG 2>&1
done
n1 "[ -f ~/sedona_geoparquet/verified.txt ] && mv ~/sedona_geoparquet/verified.txt ~/sedona_geoparquet/verified.txt.removed_20261013" >> $LOG 2>&1
log "plain: GeoParquet copies deleted"

# ---- 1b. crossing_points' plain Parquet back onto HDFS, replication 2 ----
P=/geobenchr/crossing_points.parquet
B='~/hdfs_backup/crossing_points.parquet'
n1 "$HENV /opt/hadoop/bin/hdfs dfs -rm -r -f -skipTrash $P > /dev/null 2>&1; /opt/hadoop/bin/hdfs dfs -D dfs.replication=2 -put $B /geobenchr/" >> $LOG 2>&1
hb=$(hsum $P); lb=$(lsum $B)
log "plain: crossing_points on HDFS $hb bytes, backup $lb bytes"
PLAIN_OK=0
{ [ -n "$hb" ] && [ "$hb" = "$lb" ]; } && PLAIN_OK=1
for t in flight_points ride_points geolife_points crossing_points; do
  n1 "$HENV /opt/hadoop/bin/hdfs dfs -setrep -w 2 /geobenchr/$t.parquet > /dev/null" >> $LOG 2>&1
done
n1 "$HENV /opt/hadoop/bin/hdfs dfs -du -s -h '/geobenchr/*_points.*'; df -h / | tail -1; for h in nst-2 nst-3 nst-4 nst-5; do ssh -o LogLevel=ERROR \$h 'echo \$(hostname) \$(df -h / | tail -1)'; done" >> $LOG 2>&1
DUCK_OK=1

# ---- 2a. Sedona, alone, all five nodes, plain Parquet ----
active() { n1 "curl -s http://nst-1:8088/ws/v1/cluster/metrics | python3 -c \"import json,sys; print(json.load(sys.stdin)['clusterMetrics']['activeNodes'])\""; }
if [ $PLAIN_OK = 0 ]; then
  log "Sedona: crossing_points plain Parquet not restored, not measured"
elif n1 "~/realbench/svc.sh only Sedona" >> $LOG 2>&1; then
  for i in $(seq 1 30); do nodes=$(active); [ "$nodes" = 5 ] && break; sleep 10; done
  log "Sedona: $nodes active YARN nodes, all four point tables as plain Parquet"
  if [ "$nodes" = 5 ]; then
    for SC in ais aviation cycling geolife; do
      nodes=$(active)
      [ "$nodes" = 5 ] || { log "Sedona: only $nodes active YARN nodes before $SC -- stopped, not measured further"; break; }
      log "Sedona $SC start"
      n1 "mkdir -p ~/realbench/out_fair4/Sedona && cd ~/realbench/out_fair4/Sedona && env SUT_HOST=nst-1 NUM_THREADS=1 SYSTEMS=Sedona SPARK_MASTER=yarn \
          SPARK_HOME=/opt/spark HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop SEDONA_GEOPARQUET= \
          SPARK_CONF='spark.sql.session.timeZone=UTC;spark.dynamicAllocation.enabled=false;spark.executor.instances=90;spark.executor.cores=1;spark.executor.memory=2g' \
          /opt/java/latest/bin/java -Xmx6g $(echo $JAVA_OPTS) -cp ~/realbench/benchmark_client-real52b.jar:\$HOME/bench_sedona/deps/'*' \
          benchmark_client.BenchmarkClient $SC > ${SC}_stdout.log 2>&1"
      log "Sedona $SC end rc=$?"
    done
  else
    log "Sedona: not all five nodes healthy -- not measured"
  fi
else
  log "Sedona: isolation failed, not measured"
fi

# ---- 2b. DuckDB AIS, alone ----
if [ $DUCK_OK = 1 ]; then
  if n1 "~/realbench/svc.sh only DuckDB" >> $LOG 2>&1; then
    mkdir -p $R/out_fair4/DuckDB && cd $R/out_fair4/DuckDB
    log "DuckDB ais start"
    env SUT_HOST=10.1.0.11 SUT_DB_PASSWORD=postgres NUM_THREADS=1 SYSTEMS=DuckDB \
        DUCKDB_CANCEL_CMD="ssh -o BatchMode=yes -o LogLevel=ERROR $N1 ~/realbench/svc.sh restart-duckdb >> $LOG 2>&1" \
        java $JAVA_OPTS -cp "$JAR:$DEPS/*" benchmark_client.BenchmarkClient ais > ais_stdout.log 2>&1
    log "DuckDB ais end rc=$?"
  else
    log "DuckDB: isolation failed, not measured"
  fi
fi
