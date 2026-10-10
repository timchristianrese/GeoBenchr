#!/bin/bash
# Isolated benchmark run (2026-10-09), driven from nst-inserter.
#
# For each system: every other benchmarked system is stopped on nst-1..5 (svc.sh on nst-1; SpaceTime's
# partner-run spacetimed cannot be stopped and stays up), then the client runs all four scenarios with 52
# repetitions per query, each repetition with different parameters (the first two count as warm-up and are
# excluded in the analysis), read-only, one query at a time. Sedona's client runs on nst-1 (its Spark driver
# needs the cluster network), every other system's on this host. Hardware is sampled every 5 s on nst-1..5
# and here. At the end -- also if this script fails or is killed -- every service is started again, the logs
# and hardware samples are collected and build_results.py (WARMUP_REPS=2) builds the tables and plots.
#   real_bench.sh            log: ~/realbench/run.log, client logs: ~/realbench/out/<system>/,
#                            results: ~/realbench/analysis/results/ (+ ~/realbench/realbench_results.tgz)
#   real_bench.sh resume <system> <scenario> <pid> <start_utc>
#       take over a run whose driver script was lost: wait for the client process <pid> (running <system>
#       <scenario>, started <start_utc>) to exit, then continue with the next scenario; the system stays isolated
set -u
R=$HOME/realbench
JAR=$R/benchmark_client-real52b.jar   # real52 + timeouts that cancel the query on the server (2026-10-09)
DEPS=$HOME/benchmark_client_deps
LOG=$R/run.log
N1=tim@10.1.0.11
log() { echo "$(date -Is) $*" | tee -a "$LOG"; }
n1() { ssh -o BatchMode=yes -o LogLevel=ERROR $N1 "$@"; }
JAVA_OPTS=(-Dfile.encoding=UTF-8 --add-exports=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED
  --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED
  --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED
  --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED
  --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED)
SYSTEMS_ORDER=(ClickHouse SpaceTime DuckDB PostGIS MobilityDB Sedona)

DONE=0
finish() {
  [ "$DONE" = 1 ] || log "ABORTED: run did not complete, restoring services anyway"
  log "restoring all services"
  n1 "~/realbench/svc.sh all" >> $LOG 2>&1
  n1 'for h in nst-1 nst-2 nst-3 nst-4 nst-5; do ssh -o LogLevel=ERROR $h "touch ~/hw_real/STOP"; done'
  touch $R/hw/STOP
  sleep 15
  # collect: client logs (Sedona's from nst-1), hardware samples of all nodes, phase times
  A=$R/analysis/results/raw
  mkdir -p $A/logs/real52 $A/hardware $R/out/Sedona
  scp -q -o LogLevel=ERROR "$N1:realbench/out/Sedona/*" $R/out/Sedona/ 2>/dev/null
  cp -r $R/out/. $A/logs/real52/
  n1 'rm -rf ~/realbench/hw_all && mkdir -p ~/realbench/hw_all && for h in nst-1 nst-2 nst-3 nst-4 nst-5; do scp -q -o LogLevel=ERROR "$h:hw_real/*.csv" ~/realbench/hw_all/; done'
  scp -q -o LogLevel=ERROR "$N1:realbench/hw_all/*.csv" $A/hardware/
  cp $R/hw/*.csv $A/hardware/ 2>/dev/null
  cp $R/phases.csv $LOG $A/ 2>/dev/null
  # revoke this host's benchmark key on nst-1 (its last use)
  n1 "sed -i '/geobenchr-benchmark\$/d' ~/.ssh/authorized_keys"
  (cd $R/analysis && RESULTS_DIR=$R/analysis/results WARMUP_REPS=2 python3 build_results.py > build.out 2>&1)
  log "results built (rc=$?): $(tail -8 $R/analysis/build.out | tr '\n' ' ')"
  tar -czf $R/realbench_results.tgz -C $R/analysis results build_results.py
  log "ALL DONE"
}
trap finish EXIT
trap 'exit 1' TERM INT

RESUME=0
if [ "${1:-}" = resume ]; then RESUME=1; RSYS=$2; RSC=$3; RPID=$4; RT0=$5; fi

if [ $RESUME = 1 ]; then
log "resume: waiting for $RSYS $RSC (pid $RPID) to finish"
while kill -0 $RPID 2>/dev/null; do sleep 30; done
echo "$RSYS,$RSC,$RT0,$(date -u +%Y-%m-%dT%H:%M:%SZ)" >> $R/phases.csv
log "$RSYS $RSC end (taken over)"
else
log "start"
# DuckDB's tables must be in their final order (time-bucket re-sort) before anything runs
until n1 'grep -q "resort2 done" ~/duckdb_reorder/reorder.log'; do sleep 60; done
log "duckdb re-sort: $(n1 'grep -E "2026-10-09.*(swapped|failed again)" ~/duckdb_reorder/reorder.log | tr "\n" " "')"
n1 'kill 332420 2>/dev/null; true'   # idle distributed-mode manager left over from 2026-09-11

# hardware sampling: nst-1..5 and this host, into fresh files
# (a sampler still running from an earlier attempt is stopped first; they append to existing files)
n1 'for h in nst-1 nst-2 nst-3 nst-4 nst-5; do ssh -o LogLevel=ERROR $h "mkdir -p ~/hw_real && touch ~/hw_real/STOP"; done; sleep 12; for h in nst-1 nst-2 nst-3 nst-4 nst-5; do ssh -o LogLevel=ERROR $h "rm -f ~/hw_real/STOP && (setsid nohup python3 ~/hw_sampler.py --out /home/tim/hw_real --interval 5 --hours 96 >> ~/hw_real/sampler.out 2>&1 < /dev/null &)"; done'
mkdir -p $R/hw && touch $R/hw/STOP && sleep 6 && rm -f $R/hw/STOP && (setsid nohup python3 $R/hw_sampler.py --out $R/hw --interval 5 --hours 96 >> $R/hw/sampler.out 2>&1 < /dev/null &)

echo "system,phase,start_utc,end_utc" > $R/phases.csv
fi
SKIP=$RESUME
for SYS in "${SYSTEMS_ORDER[@]}"; do
  if [ $SKIP = 1 ] && [ "$SYS" != "${RSYS:-}" ]; then continue; fi
  OUT=$R/out/$SYS; mkdir -p $OUT
  if [ $SKIP = 0 ]; then
  t0=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  if ! n1 "~/realbench/svc.sh only $SYS" >> $LOG 2>&1; then
    log "$SYS: isolation failed, retrying once"
    n1 "~/realbench/svc.sh only $SYS" >> $LOG 2>&1 || { log "$SYS: SKIPPED, services not healthy"; continue; }
  fi
  echo "$SYS,isolate,$t0,$(date -u +%Y-%m-%dT%H:%M:%SZ)" >> $R/phases.csv
  fi
  for SC in ais aviation cycling geolife; do
    if [ $SKIP = 1 ]; then [ "$SC" = "$RSC" ] && SKIP=0; continue; fi
    s0=$(date -u +%Y-%m-%dT%H:%M:%SZ); log "$SYS $SC start"
    if [ "$SYS" = Sedona ]; then
      n1 "cd ~/realbench && mkdir -p out/Sedona && cd out/Sedona && env SUT_HOST=nst-1 NUM_THREADS=1 SYSTEMS=Sedona SPARK_MASTER=yarn \
          SPARK_HOME=/opt/spark HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop SEDONA_GEOPARQUET=\$(paste -sd, ~/sedona_geoparquet/verified.txt) \
          SPARK_CONF='spark.sql.session.timeZone=UTC;spark.dynamicAllocation.enabled=false;spark.executor.instances=36;spark.executor.cores=1;spark.executor.memory=2g;spark.yarn.exclude.nodes=nst-1' \
          /opt/java/latest/bin/java -Xmx6g ${JAVA_OPTS[*]} -cp ~/realbench/benchmark_client-real52b.jar:\$HOME/bench_sedona/deps/'*' \
          benchmark_client.BenchmarkClient $SC > ${SC}_stdout.log 2>&1"
      rc=$?
    else
      (cd $OUT && env SUT_HOST=10.1.0.11 SUT_DB_PASSWORD=postgres NUM_THREADS=1 SYSTEMS=$SYS \
          DUCKDB_CANCEL_CMD="ssh -o BatchMode=yes -o LogLevel=ERROR $N1 ~/realbench/svc.sh restart-duckdb >> $LOG 2>&1" \
          SPACETIME_PYTHON=$HOME/bench/.venv/bin/python SPACETIME_RUNNER_SCRIPT=$HOME/bench/spacetime_query_runner.py SPACETIME_PORT=31340 \
          java "${JAVA_OPTS[@]}" -cp "$JAR:$DEPS/*" benchmark_client.BenchmarkClient $SC > ${SC}_stdout.log 2>&1)
      rc=$?
    fi
    echo "$SYS,$SC,$s0,$(date -u +%Y-%m-%dT%H:%M:%SZ)" >> $R/phases.csv
    log "$SYS $SC end rc=$rc"
  done
done

DONE=1
