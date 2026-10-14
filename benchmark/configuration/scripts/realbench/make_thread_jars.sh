#!/bin/bash
# Client jars for runs with 1, 4 and 8 client threads (2026-10-14). Each run measures the same 128 parameter
# sets per query and adds one warm-up repetition per client thread:
#   1. build_jar.sh generates every read query MAX_THREADS + 128 times (136), each repetition with its own
#      parameters, in one seeded order;
#   2. the jar for n threads keeps, per query, the last 128 + n of those repetitions (file order unchanged), so
#      the measured repetitions -- the last 128 -- are identical in every run, and the n before them are the
#      warm-up (build_results.py with WARMUP_REPS=n).
#   make_thread_jars.sh OUT_DIR [threads...]      (default threads: 1 4 8; jars: OUT_DIR/benchmark_client-t<n>.jar)
set -euo pipefail
OUT=$(cd "$1" && pwd); shift
THREADS=("${@:-1 4 8}"); THREADS=(${THREADS[@]})
MEASURED=128
MAX=0; for n in "${THREADS[@]}"; do [ "$n" -gt "$MAX" ] && MAX=$n; done
HERE=$(cd "$(dirname "$0")" && pwd)
T=$(mktemp -d); trap 'rm -rf "$T"' EXIT
JAR=${JAVA_HOME:+$JAVA_HOME/bin/}jar

"$HERE/build_jar.sh" "$OUT/benchmark_client-base$((MEASURED + MAX)).jar" $((MEASURED + MAX))
mkdir -p "$T/base" && (cd "$T/base" && unzip -q -o "$OUT/benchmark_client-base$((MEASURED + MAX)).jar" '*/*_queries.yaml')

for n in "${THREADS[@]}"; do
  rm -rf "$T/t$n" && cp -R "$T/base" "$T/t$n"
  python3 - "$T/t$n" $((MEASURED + n)) <<'PY'
import glob, sys, yaml
root, keep = sys.argv[1], int(sys.argv[2])
class Lit(str): pass
yaml.add_representer(Lit, lambda d, s: d.represent_scalar("tag:yaml.org,2002:str", s, style="|"))
for path in glob.glob(f"{root}/*/*_queries.yaml"):
    entries = yaml.safe_load(open(path)) or []
    total = {}
    for e in entries: total[e["name"]] = total.get(e["name"], 0) + 1
    seen, out = {}, []
    for e in entries:
        i = seen[e["name"]] = seen.get(e["name"], 0) + 1
        if i > total[e["name"]] - keep:          # the last `keep` repetitions of this query
            if isinstance(e.get("sql"), str) and "\n" in e["sql"]: e["sql"] = Lit(e["sql"])
            out.append(e)
    yaml.dump(out, open(path, "w"), sort_keys=False, allow_unicode=True)
PY
  cp "$OUT/benchmark_client-base$((MEASURED + MAX)).jar" "$OUT/benchmark_client-t$n.jar"
  (cd "$T/t$n" && "$JAR" uf "$OUT/benchmark_client-t$n.jar" */*_queries.yaml)
  echo "t$n: $(cd "$T/t$n" && for f in cycling/postgisSQL_queries.yaml geolife/mobilitydb_queries.yaml; do printf '%s=%s ' "$f" "$(grep -c '^- name:' $f)"; done)"
done
