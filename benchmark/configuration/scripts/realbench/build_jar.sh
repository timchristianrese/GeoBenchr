#!/bin/bash
# Builds the benchmark client jar with the query files of a benchmark run: every read query REPS times
# (each repetition with its own parameters), write queries off, ClickHouse with its H3 versions. Works on
# a temporary copy of benchmark/, so the tracked query files stay as they are.
#   build_jar.sh OUT.jar [REPS]        (REPS defaults to 52; needs python3 with pyyaml + pyproj, maven)
# WRITES=1 keeps the write queries (insert, update, bulk insert); GEN_WARMUP=n lets the last n repetitions of a query
# repeat parameters when it has no more distinct ones (see make_workload_jars.py)
set -euo pipefail
OUT=$(cd "$(dirname "$1")" && pwd)/$(basename "$1"); REPS=${2:-52}
HERE=$(cd "$(dirname "$0")" && pwd); BENCH=$(cd "$HERE/../../.." && pwd)
T=$(mktemp -d); trap 'rm -rf "$T"' EXIT
mkdir -p "$T/benchmark"
ln -s "$(cd "$BENCH/.." && pwd)/server" "$T/server"   # reference datasets the generators read
rsync -a --exclude target --exclude venv --exclude __pycache__ "$BENCH/configuration" "$BENCH/client" "$T/benchmark/"
C=$T/benchmark/configuration
python3 "$HERE/set_reps.py" "$C" "$REPS" h3 ${WRITES:+writes}
export GEN_WARMUP=${GEN_WARMUP:-0}
for sc in ais aviation cycling geolife; do
  (cd "$C/$sc/scripts" && python3 generateParameterVariations.py > "$T/gen_$sc.log" 2>&1) || { echo "$sc: generation failed"; tail -5 "$T/gen_$sc.log"; exit 1; }
done
(cd "$C/general/scripts" && python3 mergeGeneralIntoApps.py > "$T/merge.log" 2>&1) || { echo "general: merge failed"; tail -5 "$T/merge.log"; exit 1; }
(cd "$T/benchmark/client/benchmark_client" && mvn -q -o clean package -DskipTests)
cp "$T/benchmark/client/benchmark_client/target/benchmark_client-1.0-SNAPSHOT.jar" "$OUT"
R=$T/benchmark/client/benchmark_client/src/main/resources
for sc in ais aviation cycling geolife; do
  echo "$sc: $(for f in postgisSQL mobilitydb clickhouse duckdb spaceTimeSQL sedonaSQL; do printf '%s=%s ' $f "$(grep -c '^- name:' "$R/$sc/${f}_queries.yaml" 2>/dev/null || echo 0)"; done)"
done
echo "wrote $OUT"
