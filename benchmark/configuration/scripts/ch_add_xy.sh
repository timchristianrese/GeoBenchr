#!/bin/bash
# ClickHouse: projected EPSG:3857 coordinates as stored columns (2026-10-07).
#
# PostGIS stores points already projected to EPSG:3857, so a distance check there is plain arithmetic
# on stored values. ClickHouse stored only lon/lat and projected every row at query time (the
# webMercatorX/Y/Distance SQL functions, a log(tan()) per point). This adds x/y as MATERIALIZED
# columns -- computed once per row, with exactly the same formula the functions used -- so the
# queries can use plain sqrt(pow(x - x0, 2) + pow(y - y0, 2)) like PostGIS does. Adding a column is
# metadata-only; MATERIALIZE COLUMN then writes it for the existing rows (a mutation per part).
# Until that finishes, ClickHouse computes x/y on read, so results are identical either way.
set -euo pipefail
W=$HOME/pipeline/ch_add_xy
mkdir -p "$W"; LOG=$W/run.log
log() { echo "$(date -Is) $*" | tee -a "$LOG"; }
die() { log "FATAL: $*"; exit 1; }
ch()  { clickhouse-client --port 9500 "$@"; }
X="6378137. * radians(lon)"
Y="6378137. * log(tan(pi() / 4 + radians(lat) / 2))"

log "start"
for t in crossing_points flight_points ride_points geolife_points harbours cities; do
  ch --query "ALTER TABLE ${t}_local ON CLUSTER cluster_1
                ADD COLUMN IF NOT EXISTS x Float64 MATERIALIZED $X,
                ADD COLUMN IF NOT EXISTS y Float64 MATERIALIZED $Y" >/dev/null
  ch --query "ALTER TABLE ${t} ON CLUSTER cluster_1
                ADD COLUMN IF NOT EXISTS x Float64 MATERIALIZED $X,
                ADD COLUMN IF NOT EXISTS y Float64 MATERIALIZED $Y" >/dev/null
  log "$t: columns added"
done
for t in harbours cities geolife_points ride_points flight_points crossing_points; do
  ch --query "ALTER TABLE ${t}_local ON CLUSTER cluster_1 MATERIALIZE COLUMN x, MATERIALIZE COLUMN y" >/dev/null
  log "$t: materializing"
done
while true; do
  left=$(ch --query "SELECT count() FROM clusterAllReplicas(cluster_1, system.mutations) WHERE database = 'default' AND NOT is_done AND command LIKE '%MATERIALIZE COLUMN%'")
  failed=$(ch --query "SELECT count() FROM clusterAllReplicas(cluster_1, system.mutations) WHERE database = 'default' AND NOT is_done AND latest_fail_reason != '' AND command LIKE '%MATERIALIZE COLUMN%'")
  [ "$failed" = 0 ] || die "a mutation is failing: $(ch --query "SELECT table, latest_fail_reason FROM clusterAllReplicas(cluster_1, system.mutations) WHERE NOT is_done AND latest_fail_reason != '' LIMIT 1 FORMAT TSV")"
  [ "$left" = 0 ] && break
  log "mutations still running: $left"
  sleep 60
done
log "all parts materialized"
# check: the stored columns equal the formula the old functions computed, on every row
for t in crossing_points flight_points ride_points geolife_points harbours cities; do
  bad=$(ch --query "SELECT countIf(x != webMercatorX(lon) OR y != webMercatorY(lat)) FROM $t")
  [ "$bad" = 0 ] || die "$t: $bad rows where x/y differ from webMercatorX/Y"
  log "$t: x/y identical to webMercatorX/Y on every row"
done
log "disk free: $(ch --query "SELECT arrayStringConcat(groupArray(hostName() || ' ' || formatReadableSize(free_space)), ', ') FROM clusterAllReplicas(cluster_1, system.disks)")"
log "DONE"
