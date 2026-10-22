#!/bin/bash
# Runs a sequence of benchmark runs, one after the other (2026-10-22), driven from nst-inserter. Each run is
# real_bench_v3.sh with the jar benchmark_client-<workload>-t<n>.jar (make_workload_jars.py): every system alone on
# the cluster, all four scenarios, n client threads, the workload's queries (ro: reads only; 75 / 90: reads and
# writes, 75/25 or 90/10). This host's benchmark key stays on nst-1 until the last run ends.
#   real_bench_suite.sh <workload>:<threads> ...     e.g. real_bench_suite.sh ro:2 ro:4 ro:8 ro:1
#   EQUIVALENCE_JSON=<file>: the pre-run check against PostGIS, passed to every run (see real_bench_v3.sh)
#   log: ~/realbench/suite.log; per run ~/realbench/<workload>_t<n>.log, results ~/realbench/realbench_results_<workload>_t<n>.tgz
set -u
R=$HOME/realbench
LOG=$R/suite.log
log() { echo "$(date -Is) $*" | tee -a "$LOG"; }
RUNS=("$@")
[ ${#RUNS[@]} -gt 0 ] || { echo "usage: real_bench_suite.sh <workload>:<threads> ..."; exit 1; }
for f in real_bench_v3.sh build_results.py hw_sampler.py; do [ -f "$R/$f" ] || { log "FATAL: $R/$f missing"; exit 1; }; done
for r in "${RUNS[@]}"; do
  wl=${r%%:*}; n=${r##*:}
  [ -f "$R/benchmark_client-$wl-t$n.jar" ] || { log "FATAL: $R/benchmark_client-$wl-t$n.jar missing"; exit 1; }
done
last=${RUNS[${#RUNS[@]}-1]}
for r in "${RUNS[@]}"; do
  wl=${r%%:*}; n=${r##*:}; tag=${wl}_t$n
  log "run $tag start"
  THREADS=$n RUN_TAG=$tag JAR_NAME=benchmark_client-$wl-t$n.jar WORKLOAD=$wl \
    KEEP_KEY=$([ "$r" = "$last" ] && echo 0 || echo 1) "$R/real_bench_v3.sh"
  log "run $tag end (rc=$?): $(grep -c ' end rc=' $R/$tag.log) scenario runs, $(grep -c 'SKIPPED' $R/$tag.log) systems skipped"
done
log "ALL RUNS DONE"
