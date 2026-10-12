#!/bin/bash
# Sedona re-run on the whole cluster (2026-10-11), run on nst-1. The 52-repetition run gave Sedona 36
# single-core executors on three nodes (nst-1 excluded, nst-4 unhealthy: disk above YARN's 90 % default);
# this re-run uses all five nodes (disk threshold raised to 95 % in yarn-site.xml) with 90 executors,
# ~18 per node, otherwise exactly as before: Sedona alone on the cluster, the same 52-repetition query
# files (jar real52b), hardware sampled every 5 s. Every service is started again at the end -- also if
# this script fails or is killed.
#   sedona_rerun.sh        log: ~/realbench/sedona5.log, client logs: ~/realbench/out_sedona5/,
#                          hardware: ~/hw_sedona5/ on nst-1..5
set -u
R=$HOME/realbench
LOG=$R/sedona5.log
OUT=$R/out_sedona5
NODES=(nst-1 nst-2 nst-3 nst-4 nst-5)
EXECUTORS=90
export JAVA_HOME=/opt/java/latest HADOOP_HOME=/opt/hadoop HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
log() { echo "$(date -Is) $*" >> "$LOG"; }
on() { ssh -o LogLevel=ERROR "$1" "$2"; }

finish() {
  log "restoring all services"
  $R/svc.sh all >> $LOG 2>&1
  for h in "${NODES[@]}"; do on $h "touch ~/hw_sedona5/STOP"; done
  log "ALL DONE"
}
trap finish EXIT
trap 'exit 1' TERM INT

log "start"
for h in "${NODES[@]}"; do
  on $h "mkdir -p ~/hw_sedona5 && rm -f ~/hw_sedona5/STOP && (setsid nohup python3 ~/hw_sampler.py --out /home/tim/hw_sedona5 --interval 5 --hours 48 >> ~/hw_sedona5/sampler.out 2>&1 < /dev/null &)"
done

$R/svc.sh only Sedona >> $LOG 2>&1 || { log "FATAL: Sedona not healthy"; exit 1; }
for i in $(seq 1 60); do
  n=$(curl -s http://nst-1:8088/ws/v1/cluster/metrics | python3 -c "import json,sys; print(json.load(sys.stdin)['clusterMetrics']['activeNodes'])" 2>/dev/null)
  [ "$n" = 5 ] && break; sleep 10
done
log "YARN: ${n:-?} active nodes; $(/opt/hadoop/bin/yarn node -list -all 2>/dev/null | grep -c UNHEALTHY) unhealthy"
[ "${n:-0}" = 5 ] || { log "FATAL: not all five nodes active in YARN"; exit 1; }

mkdir -p $OUT && cd $OUT
echo "system,phase,start_utc,end_utc" > $OUT/phases.csv
for SC in ais aviation cycling geolife; do
  s0=$(date -u +%Y-%m-%dT%H:%M:%SZ); log "Sedona $SC start"
  env SUT_HOST=nst-1 NUM_THREADS=1 SYSTEMS=Sedona SPARK_MASTER=yarn SPARK_HOME=/opt/spark HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop \
      SEDONA_GEOPARQUET=$(paste -sd, ~/sedona_geoparquet/verified.txt) \
      SPARK_CONF="spark.sql.session.timeZone=UTC;spark.dynamicAllocation.enabled=false;spark.executor.instances=$EXECUTORS;spark.executor.cores=1;spark.executor.memory=2g" \
    /opt/java/latest/bin/java -Xmx6g -Dfile.encoding=UTF-8 \
    --add-exports=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
    --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED \
    --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
    --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED \
    --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED \
    -cp "$R/benchmark_client-real52b.jar:$HOME/bench_sedona/deps/*" benchmark_client.BenchmarkClient $SC > ${SC}_stdout.log 2>&1
  rc=$?
  echo "Sedona,$SC,$s0,$(date -u +%Y-%m-%dT%H:%M:%SZ)" >> $OUT/phases.csv
  log "Sedona $SC end rc=$rc"
done
