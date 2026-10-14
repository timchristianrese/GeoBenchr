#!/bin/bash
# Re-run after the partner review (prepared 2026-10-14), driven from nst-inserter. Same procedure as
# real_bench.sh (2026-10-09): each system alone on nst-1..5 (svc.sh on nst-1 stops every other benchmarked
# system and now restarts the one under test; SpaceTime's partner-run spacetimed stays up), all four scenarios,
# 52 repetitions per query with different parameters (the first two are warm-up), read-only, one query at a
# time, hardware sampled every 5 s. What changed since that run:
#   - jar benchmark_client-v3.jar (build_jar.sh): the query fixes of the review (cycling regions and time
#     bounds, geolife time bounds, MobilityDB parameter order, CountRidesFullyInDistrictRadius for every system
#     incl. ClickHouse, SpaceTime's busiestDayInWeekPeriod and AIS time filters, ClickHouse pointInPolygon for
#     the geolife regions), DuckDB no longer prints rows inside the timed window, Postgres fetches adaptively
#   - data: geolife complete (3,960,126,695 rows) in every system; the 20 test rides (ids 1-20) removed
#   - Postgres: same settings on every node (nst-3 had defaults), nst-3's shards partitioned like the others,
#     point tables clustered on their GiST index; DuckDB in the v1.5.0 storage format (row-group bounding boxes
#     kept); ClickHouse without its own 10 GB per-query limit; Sedona on spatially partitioned GeoParquet
#     (all four point tables) with 20 executors x 5 cores x 11 GB (YARN: 54 GB, 20 vcores per node)
# Before starting: this host's benchmark key must be in nst-1's ~/.ssh/authorized_keys (finish() removes it),
# and the jar must be in ~/realbench on this host and on nst-1.
#   real_bench_v3.sh     log: ~/realbench/run3.log, client logs: ~/realbench/out3/<system>/,
#                        results: ~/realbench/analysis3/results/ (+ ~/realbench/realbench_results3.tgz)
set -u
R=$HOME/realbench
JAR=$R/benchmark_client-v3.jar
DEPS=$HOME/benchmark_client_deps
LOG=$R/run3.log
OUT3=$R/out3
PHASES=$R/phases3.csv
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
SEDONA_CONF='spark.sql.session.timeZone=UTC;spark.dynamicAllocation.enabled=false;spark.executor.instances=20;spark.executor.cores=5;spark.executor.memory=11g'

DONE=0
finish() {
  [ "$DONE" = 1 ] || log "ABORTED: run did not complete, restoring services anyway"
  log "restoring all services"
  n1 "~/realbench/svc.sh all" >> $LOG 2>&1
  n1 'for h in nst-1 nst-2 nst-3 nst-4 nst-5; do ssh -o LogLevel=ERROR $h "touch ~/hw_run3/STOP"; done'
  touch $R/hw3/STOP
  sleep 15
  # collect: client logs (Sedona's from nst-1), query files of the jar, hardware samples, phase times
  A=$R/analysis3/results/raw
  mkdir -p $A/logs/run3 $A/hardware $A/queries $OUT3/Sedona
  scp -q -o LogLevel=ERROR "$N1:realbench/out3/Sedona/*" $OUT3/Sedona/ 2>/dev/null
  cp -r $OUT3/. $A/logs/run3/
  (cd $A/queries && unzip -q -o $JAR '*/*_queries.yaml')
  n1 'rm -rf ~/realbench/hw_all3 && mkdir -p ~/realbench/hw_all3 && for h in nst-1 nst-2 nst-3 nst-4 nst-5; do scp -q -o LogLevel=ERROR "$h:hw_run3/*.csv" ~/realbench/hw_all3/; done'
  scp -q -o LogLevel=ERROR "$N1:realbench/hw_all3/*.csv" $A/hardware/
  cp $R/hw3/*.csv $A/hardware/ 2>/dev/null
  cp $PHASES $A/phases.csv; cp $LOG $A/ 2>/dev/null
  # revoke this host's benchmark key on nst-1 (its last use)
  n1 "sed -i '/geobenchr-benchmark\$/d' ~/.ssh/authorized_keys"
  cp $R/analysis/build_results.py $R/analysis3/ 2>/dev/null
  (cd $R/analysis3 && RESULTS_DIR=$R/analysis3/results WARMUP_REPS=2 python3 build_results.py > build.out 2>&1)
  log "results built (rc=$?): $(tail -8 $R/analysis3/build.out | tr '\n' ' ')"
  tar -czf $R/realbench_results3.tgz -C $R/analysis3 results build_results.py
  log "ALL DONE"
}
trap finish EXIT
trap 'exit 1' TERM INT

log "start (jar $(basename $JAR), $(md5sum < $JAR | cut -c1-8))"
n1 "md5sum < ~/realbench/$(basename $JAR) | cut -c1-8" | grep -q "$(md5sum < $JAR | cut -c1-8)" || { log "FATAL: the jar on nst-1 differs from this host's"; exit 1; }

# hardware sampling: nst-1..5 and this host, into fresh directories
n1 'for h in nst-1 nst-2 nst-3 nst-4 nst-5; do ssh -o LogLevel=ERROR $h "mkdir -p ~/hw_run3 && touch ~/hw_run3/STOP"; done; sleep 12; for h in nst-1 nst-2 nst-3 nst-4 nst-5; do ssh -o LogLevel=ERROR $h "rm -f ~/hw_run3/STOP && (setsid nohup python3 ~/hw_sampler.py --out /home/tim/hw_run3 --interval 5 --hours 120 >> ~/hw_run3/sampler.out 2>&1 < /dev/null &)"; done'
mkdir -p $R/hw3 && touch $R/hw3/STOP && sleep 6 && rm -f $R/hw3/STOP && (setsid nohup python3 $R/hw_sampler.py --out $R/hw3 --interval 5 --hours 120 >> $R/hw3/sampler.out 2>&1 < /dev/null &)

echo "system,phase,start_utc,end_utc" > $PHASES
for SYS in "${SYSTEMS_ORDER[@]}"; do
  OUT=$OUT3/$SYS; mkdir -p $OUT
  t0=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  if ! n1 "~/realbench/svc.sh only $SYS" >> $LOG 2>&1; then
    log "$SYS: isolation failed, retrying once"
    n1 "~/realbench/svc.sh only $SYS" >> $LOG 2>&1 || { log "$SYS: SKIPPED, services not healthy"; continue; }
  fi
  echo "$SYS,isolate,$t0,$(date -u +%Y-%m-%dT%H:%M:%SZ)" >> $PHASES
  for SC in ais aviation cycling geolife; do
    s0=$(date -u +%Y-%m-%dT%H:%M:%SZ); log "$SYS $SC start"
    if [ "$SYS" = Sedona ]; then
      n1 "cd ~/realbench && mkdir -p out3/Sedona && cd out3/Sedona && env SUT_HOST=nst-1 NUM_THREADS=1 SYSTEMS=Sedona SPARK_MASTER=yarn \
          SPARK_HOME=/opt/spark HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop SEDONA_GEOPARQUET=crossing_points,flight_points,ride_points,geolife_points \
          SPARK_CONF='$SEDONA_CONF' \
          /opt/java/latest/bin/java -Xmx6g ${JAVA_OPTS[*]} -cp ~/realbench/$(basename $JAR):\$HOME/bench_sedona/deps/'*' \
          benchmark_client.BenchmarkClient $SC > ${SC}_stdout.log 2>&1"
      rc=$?
    else
      (cd $OUT && env SUT_HOST=10.1.0.11 SUT_DB_PASSWORD=postgres NUM_THREADS=1 SYSTEMS=$SYS \
          DUCKDB_CANCEL_CMD="ssh -o BatchMode=yes -o LogLevel=ERROR $N1 ~/realbench/svc.sh restart-duckdb >> $LOG 2>&1" \
          SPACETIME_PYTHON=$HOME/bench/.venv/bin/python SPACETIME_RUNNER_SCRIPT=$HOME/bench/spacetime_query_runner.py SPACETIME_PORT=31340 \
          java "${JAVA_OPTS[@]}" -cp "$JAR:$DEPS/*" benchmark_client.BenchmarkClient $SC > ${SC}_stdout.log 2>&1)
      rc=$?
    fi
    echo "$SYS,$SC,$s0,$(date -u +%Y-%m-%dT%H:%M:%SZ)" >> $PHASES
    log "$SYS $SC end rc=$rc"
  done
done

DONE=1
