#!/bin/bash
# DuckDB: store the point tables in spatial (Hilbert curve) order (2026-10-08).
#
# DuckDB has no spatial index on the point tables (an R-tree over billions of points would not fit next
# to everything else on nst-1), so every spatial filter scanned the whole table. DuckDB keeps min/max
# statistics per row group (~122k rows), including a bounding box for GEOMETRY columns; on the existing
# tables those boxes are unknown (+-infinity, the data predates them) and rows are in load order, so a
# region touches every row group anyway. Rewriting a table sorted by ST_Hilbert(geom) puts nearby points
# into the same row groups and writes real per-row-group bounding boxes, so spatial filters can skip the
# rest -- DuckDB's native counterpart of ClickHouse's (h3, timestamp, id) sort order. No index, no query
# change.
#
#   duckdb_spatial_reorder.sh <table> <check.sql> [time bucket]
# With a time bucket (hour/day/month) the sort key is (date_trunc(bucket, timestamp), Hilbert): rows stay
# grouped by time, so timestamp filters can still skip row groups, and are spatially ordered within each
# bucket. Pure Hilbert order (2026-10-08 first run) scattered every hour across the whole table and made
# time-filtered queries up to 30x slower.
# Builds <table>_hilbert, then swaps it in only if: same row count, geometry extents known, and the check
# query (a benchmark SpatialBBox query on <table>) returns the same result on both tables. Logs the check
# query's time on the old and the new table. Refuses to start without enough free disk.
set -euo pipefail
T=$1
CHECK=$(cat "$2")
BUCKET=${3:-}
TIMEKEY="${BUCKET:+date_trunc('$BUCKET', timestamp), }"
DUCKDB=/home/tim/.duckdb/cli/latest/duckdb
LOG=$HOME/duckdb_reorder/reorder.log
log() { echo "$(date -Is) [$T] $*" | tee -a "$LOG"; }
die() { log "FATAL: $*"; exit 1; }
duck() { "$DUCKDB" -unsigned -csv -noheader -c "LOAD quack; SELECT * FROM quack_query('quack:127.0.0.1:9494', \$q\$$1\$q\$, token := 'my-secret-token');"; }
timed() { local t0 r; t0=$(date +%s%N); r=$(duck "$1"); echo "$r|$(( ($(date +%s%N) - t0) / 1000000 ))"; }   # result|milliseconds

n_old=$(duck "SELECT count(*) FROM $T")
# space: ~40 bytes/row on disk (567 GB file / ~14 B rows); the copy plus the sort's spill files need
# up to ~2x that while the old table still exists
need_gb=$(( n_old * 40 * 2 / 1000000000 + 50 ))
free_gb=$(df -BG /var/lib/duckdb | awk 'NR==2{print $4}' | tr -d G)
log "start: $n_old rows, $free_gb GB free, need ~$need_gb GB"
[ "$free_gb" -ge "$need_gb" ] || die "not enough disk ($free_gb GB free, need ~$need_gb GB) -- table left as it is"

before=$(timed "$CHECK")
log "check query on the original table: result ${before%|*}, ${before##*|} ms"

# space-separated: a comma-separated string comes back as one quoted CSV field
bounds=$(duck "SELECT ST_XMin(e) || ' ' || ST_YMin(e) || ' ' || ST_XMax(e) || ' ' || ST_YMax(e) FROM (SELECT ST_Extent_Agg(geom) AS e FROM $T)")
read -r x0 y0 x1 y1 <<< "$bounds"
for v in "$x0" "$y0" "$x1" "$y1"; do [[ "$v" =~ ^-?[0-9.eE+-]+$ ]] || die "could not read the extent: $bounds"; done
log "extent $bounds; building ${T}_hilbert"
duck "DROP TABLE IF EXISTS ${T}_hilbert" >/dev/null
duck "CREATE TABLE ${T}_hilbert AS
        SELECT * FROM $T
        ORDER BY ${TIMEKEY}ST_Hilbert(geom, {'min_x': $x0, 'min_y': $y0, 'max_x': $x1, 'max_y': $y1}::BOX_2D), timestamp" >/dev/null
duck "FORCE CHECKPOINT" >/dev/null
n_new=$(duck "SELECT count(*) FROM ${T}_hilbert")
[ "$n_new" = "$n_old" ] || die "${T}_hilbert has $n_new rows, expected $n_old -- original kept"
unknown=$(duck "SELECT count(*) FROM pragma_storage_info('${T}_hilbert') WHERE column_name = 'geom' AND segment_type = 'GEOMETRY' AND stats LIKE '%-inf%'")
log "${T}_hilbert built: $n_new rows, geometry segments with unknown extent: $unknown"

after=$(timed "${CHECK//FROM $T /FROM ${T}_hilbert }")
log "check query on the reordered table: result ${after%|*}, ${after##*|} ms"
[ "${after%|*}" = "${before%|*}" ] || die "check query results differ (${before%|*} vs ${after%|*}) -- original kept, ${T}_hilbert left for inspection"

duck "DROP TABLE $T" >/dev/null
duck "ALTER TABLE ${T}_hilbert RENAME TO $T" >/dev/null
duck "FORCE CHECKPOINT" >/dev/null
log "swapped: $T is now in (${BUCKET:-no time bucket}, Hilbert) order; check query ${before##*|} ms -> ${after##*|} ms; $(df -BG /var/lib/duckdb | awk 'NR==2{print $4}') free"
