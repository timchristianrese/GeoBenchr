#!/usr/bin/env bash
set -euo pipefail

PGHOST="${PGHOST:-localhost}"
PGPORT="${PGPORT:-5432}"
PGUSER="${PGUSER:-postgres}"
PGDATABASE="${PGDATABASE:-my_base}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BOOTSTRAP="${SCRIPT_DIR}/databasetst/bootstrap_databases.sh"
GEN_QUERIES="${SCRIPT_DIR}/tst/generate_test_queries.sh"

OUT_DIR="${OUT_DIR:-${SCRIPT_DIR}/tstqueries}"

NORM_PY="${NORM_PY:-${SCRIPT_DIR}/normalize_results.py}"
COMPARE_PY="${COMPARE_PY:-${SCRIPT_DIR}/compare_results.py}"

[[ -x "$BOOTSTRAP" ]] || { echo "[!] Not executable: $BOOTSTRAP" >&2; exit 1; }
[[ -x "$GEN_QUERIES" ]] || { echo "[!] Not executable: $GEN_QUERIES" >&2; exit 1; }
[[ -f "$NORM_PY"    ]] || { echo "[!] Not found: $NORM_PY" >&2; exit 1; }
[[ -f "$COMPARE_PY" ]] || { echo "[!] Not found: $COMPARE_PY" >&2; exit 1; }

echo "[1/4] Bootstrapping databases (PostGIS + MongoDB)..."
"$BOOTSTRAP"

echo "[2/4] Generating test queries..."
"$GEN_QUERIES"

[[ -d "$OUT_DIR" ]] || { echo "[!] Output dir not found: $OUT_DIR" >&2; exit 1; }

echo "[3/4] Running & comparing results..."
pass=0
fail=0
total=0

shopt -s globstar nullglob
for qdir in "$OUT_DIR"/**/; do
    [[ -d "$qdir" ]] || continue
    pgq="$qdir/postgis.sql"
    mgp="$qdir/mongodb.py"
    [[ -f "$pgq" && -f "$mgp" ]] || continue

    total=$((total+1))
    echo "  -> $(basename "$qdir")"

    # POSTGIS
    pg_csv="$qdir/postgis.csv"
    pg_json="$qdir/postgis.json"
    PGPASSWORD="${PGPASSWORD:-}" psql -v ON_ERROR_STOP=1 \
        --no-psqlrc --quiet --csv --pset=footer=off\
        -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" \
        -f "$pgq" > "$pg_csv"
    python3 "$NORM_PY" csv --in "$pg_csv" --out "$pg_json"

    # MONGODB
    mg_raw="$qdir/mongodb.raw.txt"
    mg_json="$qdir/mongodb.json"
    python3 "$mgp" > "$mg_raw"
    python3 "$NORM_PY" mongo --in "$mg_raw" --out "$mg_json"

    # FILE EMPTY?
    pg_len=$(wc -c < "$pg_json")
    mg_len=$(wc -c < "$mg_json")

    if [[ $pg_len -le 3 || $mg_len -le 3 ]]; then
        echo "    [!] Empty result (pg:$pg_len bytes, mg:$mg_len bytes)"
        fail=$((fail+1))
        continue
    fi

    # COMPARE
    if python3 "$COMPARE_PY" --a "$pg_json" --b "$mg_json"; then
        echo "    OK"
        pass=$((pass+1))
    else
        echo "    DIFF (voir $cmp_out)"
        fail=$((fail+1))
    fi

    rm -f "$pg_csv" "$pg_json" "$mg_raw" "$mg_json"|| true

done
shopt -u globstar nullglob

echo "[4/4] Summary: total=$total  pass=$pass  fail=$fail"
if (( fail > 0 )); then
  exit 2
fi