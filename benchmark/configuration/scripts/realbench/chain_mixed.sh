#!/bin/bash
# After the read-only runs (2026-10-24), driven from nst-inserter: waits for the running real_bench_suite.sh to end,
# runs a short write trial (3 of each write query per scenario, one client thread) on the systems with writes,
# with purge_writes.sh before and after each, and starts the mixed workloads only if the trial had no failed query
# and every system's tables were cleaned. The extra key line "geobenchr-benchmark-chain" on nst-1 (kept by the
# read-only suite's last run) is removed at the end.
#   chain_mixed.sh      log: ~/realbench/chain.log
set -u
R=$HOME/realbench
LOG=$R/chain.log
SYSTEMS="ClickHouse DuckDB PostGIS MobilityDB"
log() { echo "$(date -Is) $*" | tee -a "$LOG"; }
finish() { ssh -o BatchMode=yes -o LogLevel=ERROR tim@10.1.0.11 "sed -i '/geobenchr-benchmark-chain\$/d' ~/.ssh/authorized_keys"; log "chain key removed"; }
trap finish EXIT

while pgrep -f real_bench_suite.sh > /dev/null; do sleep 300; done
log "read-only suite finished: $(tail -1 $R/suite.log)"

THREADS=1 RUN_TAG=trial_writes JAR_NAME=benchmark_client-trialw.jar WORKLOAD=75 SYSTEMS_LIST="$SYSTEMS" KEEP_KEY=1 \
  EQUIVALENCE_JSON=$R/equivalence_ro4.json "$R/real_bench_v3.sh"
failed=$(grep -rhE "\] (ERROR|TIMEOUT|INTERRUPTED):" $R/out_trial_writes 2>/dev/null | wc -l)
purge_bad=$(grep -cE "PURGE FAILED|SKIPPED" $R/trial_writes.log)
ok=$(grep -c "writes purged" $R/trial_writes.log)
log "trial: $failed failed executions, $purge_bad purge failures/skipped systems, $ok of 4 systems purged"
grep -E "\[purge" $R/trial_writes.log | tail -40 >> $LOG
if [ "$failed" = 0 ] && [ "$purge_bad" = 0 ] && [ "$ok" = 4 ]; then
  log "trial passed: starting the mixed workloads"
  SYSTEMS_LIST="$SYSTEMS" EQUIVALENCE_JSON=$R/equivalence_ro4.json "$R/real_bench_suite.sh" 75:2 75:4 75:8 75:1 90:2 90:4 90:8 90:1
  log "mixed workloads finished: $(tail -1 $R/suite.log)"
else
  log "TRIAL FAILED: mixed workloads not started"
  grep -rhE "\] (ERROR|TIMEOUT|INTERRUPTED):" $R/out_trial_writes 2>/dev/null | cut -c1-300 | sort | uniq | head -30 >> $LOG
fi
