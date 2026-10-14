#!/bin/bash
# Runs the benchmark with 1, 4 and 8 client threads, one after the other (2026-10-14), driven from
# nst-inserter. Each run is real_bench_v3.sh with THREADS=n and the jar benchmark_client-t<n>.jar
# (make_thread_jars.sh): every system alone on the cluster, all four scenarios, per query the same 128 measured
# parameter sets plus n warm-up repetitions. This host's benchmark key stays on nst-1 until the last run ends.
#   real_bench_threads.sh [threads...]      (default: 1 4 8)
#   log: ~/realbench/run3_threads.log; per run see real_bench_v3.sh
set -u
R=$HOME/realbench
THREADS=("${@:-1 4 8}"); THREADS=(${THREADS[@]})
LOG=$R/run3_threads.log
log() { echo "$(date -Is) $*" | tee -a "$LOG"; }
for f in real_bench_v3.sh build_results.py hw_sampler.py; do [ -f "$R/$f" ] || { log "FATAL: $R/$f missing"; exit 1; }; done
for n in "${THREADS[@]}"; do [ -f "$R/benchmark_client-t$n.jar" ] || { log "FATAL: $R/benchmark_client-t$n.jar missing"; exit 1; }; done
last=${THREADS[${#THREADS[@]}-1]}
for n in "${THREADS[@]}"; do
  log "run with $n client thread(s) start"
  THREADS=$n KEEP_KEY=$([ "$n" = "$last" ] && echo 0 || echo 1) "$R/real_bench_v3.sh"
  log "run with $n client thread(s) end (rc=$?): $(grep -c ' end rc=' $R/run3_t$n.log) scenario runs, $(grep -c 'SKIPPED' $R/run3_t$n.log) systems skipped"
done
log "ALL RUNS DONE"
