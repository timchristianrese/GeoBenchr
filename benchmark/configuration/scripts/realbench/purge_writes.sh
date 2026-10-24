#!/bin/bash
# Removes what a benchmark run's write queries added (2026-10-24), run on nst-1 after each system's mixed-workload
# run. Every row a write query inserts or updates has a negative id (ScratchIdPool in generateParameterVariations.py),
# every real row a non-negative one, so deleting id < 0 restores each table exactly; the check afterwards must count 0.
#   purge_writes.sh PostGIS|MobilityDB|ClickHouse|DuckDB      (exit 1 if rows with id < 0 are left)
# PostGIS and MobilityDB share the Citus cluster: both purge the point tables and the trajectory tables, then
# VACUUM (ANALYZE) them (dead tuples, planner statistics). ClickHouse: one synchronous mutation per table on every
# shard. DuckDB: with its server stopped, on the database file, then CHECKPOINT; the server is started again.
set -u
SYS=$1
POINTS="crossing_points flight_points ride_points geolife_points"
TRAJ="crossings flights rides"
log() { echo "$(date -Is) [purge $SYS] $*"; }
pg() { PGPASSWORD=postgres psql -h localhost -p 5433 -U postgres -d postgres -v ON_ERROR_STOP=1 -Atc "$1"; }
left=0
case $SYS in
  PostGIS|MobilityDB)
    for t in $POINTS $TRAJ; do
      n=$(pg "SELECT count(*) FROM $t WHERE id < 0") || exit 1
      if [ "$n" != 0 ]; then
        pg "DELETE FROM $t WHERE id < 0" > /dev/null && pg "VACUUM (ANALYZE) $t" > /dev/null || exit 1
      fi
      after=$(pg "SELECT count(*) FROM $t WHERE id < 0"); log "$t: $n rows removed, $after left"
      left=$((left + after))
    done ;;
  ClickHouse)
    for t in $POINTS; do
      n=$(clickhouse-client --port 9500 --query "SELECT count() FROM $t WHERE id < 0") || exit 1
      if [ "$n" != 0 ]; then
        clickhouse-client --port 9500 --query "ALTER TABLE ${t}_local ON CLUSTER cluster_1 DELETE WHERE id < 0 SETTINGS mutations_sync = 2" > /dev/null || exit 1
      fi
      after=$(clickhouse-client --port 9500 --query "SELECT count() FROM $t WHERE id < 0"); log "$t: $n rows removed, $after left"
      left=$((left + after))
    done ;;
  DuckDB)
    sudo systemctl stop duckdb-quack
    DB=/var/lib/duckdb/geobenchr.duckdb
    for t in $POINTS; do
      n=$(/home/tim/.duckdb/cli/latest/duckdb -csv -noheader "$DB" -c "SELECT count(*) FROM $t WHERE id < 0" 2>&1 | tail -1)
      [ "$n" != 0 ] && /home/tim/.duckdb/cli/latest/duckdb "$DB" -c "DELETE FROM $t WHERE id < 0; CHECKPOINT;" > /dev/null
      after=$(/home/tim/.duckdb/cli/latest/duckdb -csv -noheader "$DB" -c "SELECT count(*) FROM $t WHERE id < 0" 2>&1 | tail -1)
      log "$t: $n rows removed, $after left"
      left=$((left + after))
    done
    ~/realbench/svc.sh only DuckDB > /dev/null ;;
  *) log "nothing to purge"; exit 0 ;;
esac
log "done, $left rows with id < 0 left"
[ "$left" = 0 ]
