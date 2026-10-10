#!/bin/bash
# Reset the benchmark's write-query side effects. Run on nst-1 (the Citus coordinator, which also
# hosts ClickHouse and the DuckDB quack server) after any benchmark run that included the
# insert/update queries.
#
# Every benchmark write targets a negative test id (inserts create ids -1..-23, updates only touch
# those ids), while real data ids are always >= 0, so deleting id < 0 restores the pure dataset.
# MobilityDB writes go to the same Postgres point tables as PostGIS; the trajectory tables
# (crossings/flights/rides) are never written by the benchmark.
#
# Usage: ./reset_benchmark_writes.sh
set -euo pipefail

TABLES="crossing_points flight_points ride_points geolife_points"
PG="psql -h localhost -p 5433 -U postgres -d postgres -Atc"
export PGPASSWORD="${SUT_DB_PASSWORD:-postgres}"
DUCKDB="${DUCKDB_CLI:-$HOME/.duckdb/cli/latest/duckdb}"
QUACK_TOKEN="${QUACK_TOKEN:-my-secret-token}"

duck() {
  "$DUCKDB" -unsigned -csv -noheader -c \
    "LOAD quack; SELECT * FROM quack_query('quack:127.0.0.1:9494', \$q\$$1\$q\$, token := '$QUACK_TOKEN');" | tail -1
}

for t in $TABLES; do
  echo "Postgres   $t: $($PG "DELETE FROM $t WHERE id < 0")"
  # ClickHouse ids are UInt64, so negative test ids are stored wrapped
  clickhouse-client --port 9500 --query \
    "DELETE FROM default.${t}_local ON CLUSTER cluster_1 WHERE reinterpretAsInt64(id) < 0" > /dev/null
  echo "ClickHouse $t: deleted"
  echo "DuckDB     $t: $(duck "DELETE FROM $t WHERE id < 0") rows deleted"
done

echo
echo "Remaining rows (all three systems should agree, with 0 negative ids):"
for t in $TABLES; do
  echo "$t | Postgres $($PG "SELECT COUNT(*) FILTER (WHERE id < 0) || ' neg / ' || COUNT(*) FROM $t")" \
       "| ClickHouse $(clickhouse-client --port 9500 --query "SELECT toString(countIf(reinterpretAsInt64(id) < 0)) || ' neg / ' || toString(count()) FROM $t")" \
       "| DuckDB $(duck "SELECT COUNT(*) FILTER (WHERE id < 0) || ' neg / ' || COUNT(*) FROM $t")"
done
