#!/bin/bash
# DuckDB spatial reorder, one month at a time (2026-10-08) -- for tables too large to sort in one go.
#
# Same goal and checks as duckdb_spatial_reorder.sh, but the copy is built month by month: each month's
# rows are inserted sorted by ST_Hilbert(geom), so the table ends up ordered by (month, Hilbert). Every
# sort then needs only that month's spill space (~25 GB for crossing_points instead of several hundred GB
# for one global sort, which would have filled the disk Postgres and ClickHouse share). Row groups are
# still spatially tight within each month, so ST_Intersects_Extent can skip the ones outside an area.
#
# With a bucket (2026-10-11; hour or day -- the months stay the chunks), each month is sorted by
# (bucket, Hilbert) instead, so the table ends up ordered by (bucket, Hilbert): the rule for all DuckDB
# point tables is the calendar unit whose buckets hold closest to 10 million rows.
#
#   duckdb_spatial_reorder_chunked.sh <table> <check.sql> [hour|day]
set -euo pipefail
T=$1
CHECK=$(cat "$2")
BUCKET=${3:-}
TIMEKEY="${BUCKET:+date_trunc('$BUCKET', timestamp), }"
DUCKDB=/home/tim/.duckdb/cli/latest/duckdb
LOG=$HOME/duckdb_reorder/reorder.log
MIN_FREE_GB=150
log() { echo "$(date -Is) [$T] $*" | tee -a "$LOG"; }
die() { log "FATAL: $*"; exit 1; }
duck() { "$DUCKDB" -unsigned -csv -noheader -c "LOAD quack; SELECT * FROM quack_query('quack:127.0.0.1:9494', \$q\$$1\$q\$, token := 'my-secret-token');"; }
timed() { local t0 r; t0=$(date +%s%N); r=$(duck "$1"); echo "$r|$(( ($(date +%s%N) - t0) / 1000000 ))"; }   # result|milliseconds
free_gb() { df -BG /var/lib/duckdb | awk 'NR==2{print $4}' | tr -d G; }

n_old=$(duck "SELECT count(*) FROM $T")
log "chunked start: $n_old rows, $(free_gb) GB free"
before=$(timed "$CHECK")
log "check query on the original table: result ${before%|*}, ${before##*|} ms"
bounds=$(duck "SELECT ST_XMin(e) || ' ' || ST_YMin(e) || ' ' || ST_XMax(e) || ' ' || ST_YMax(e) FROM (SELECT ST_Extent_Agg(geom) AS e FROM $T)")
read -r x0 y0 x1 y1 <<< "$bounds"
for v in "$x0" "$y0" "$x1" "$y1"; do [[ "$v" =~ ^-?[0-9.eE+-]+$ ]] || die "could not read the extent: $bounds"; done
HILBERT="ST_Hilbert(geom, {'min_x': $x0, 'min_y': $y0, 'max_x': $x1, 'max_y': $y1}::BOX_2D)"
months=$(duck "SELECT strftime(m, '%Y-%m-%d') FROM (SELECT DISTINCT date_trunc('month', timestamp) AS m FROM $T WHERE timestamp IS NOT NULL) ORDER BY m")
log "extent $bounds; $(echo "$months" | wc -l) months"
duck "DROP TABLE IF EXISTS ${T}_hilbert" >/dev/null
duck "CREATE TABLE ${T}_hilbert AS SELECT * FROM $T LIMIT 0" >/dev/null
for m in $months; do
  [ "$(free_gb)" -ge "$MIN_FREE_GB" ] || die "only $(free_gb) GB free before month $m -- original kept, ${T}_hilbert incomplete"
  duck "INSERT INTO ${T}_hilbert SELECT * FROM $T
          WHERE timestamp >= TIMESTAMP '$m' AND timestamp < TIMESTAMP '$m' + INTERVAL 1 MONTH
          ORDER BY ${TIMEKEY}$HILBERT, timestamp" >/dev/null
  duck "FORCE CHECKPOINT" >/dev/null
  log "month $m done ($(free_gb) GB free)"
done
duck "INSERT INTO ${T}_hilbert SELECT * FROM $T WHERE timestamp IS NULL ORDER BY $HILBERT" >/dev/null
duck "FORCE CHECKPOINT" >/dev/null
n_new=$(duck "SELECT count(*) FROM ${T}_hilbert")
[ "$n_new" = "$n_old" ] || die "${T}_hilbert has $n_new rows, expected $n_old -- original kept"
unknown=$(duck "SELECT count(*) FROM pragma_storage_info('${T}_hilbert') WHERE column_name = 'geom' AND segment_type = 'GEOMETRY' AND stats LIKE '%-inf%'")
log "${T}_hilbert built: $n_new rows, geometry segments with unknown extent: $unknown"
after=$(timed "${CHECK//FROM $T /FROM ${T}_hilbert }")
log "check query on the reordered table: result ${after%|*}, ${after##*|} ms"
[ "${after%|*}" = "${before%|*}" ] || die "check query results differ (${before%|*} vs ${after%|*}) -- original kept"
duck "DROP TABLE $T" >/dev/null
duck "ALTER TABLE ${T}_hilbert RENAME TO $T" >/dev/null
duck "FORCE CHECKPOINT" >/dev/null
log "swapped: $T is now in (${BUCKET:-month}, Hilbert) order; check query ${before##*|} ms -> ${after##*|} ms; $(free_gb) GB free"
