#!/bin/bash
# =====================================================================================================
# AIS → UTC migration (GeoBenchr cluster, run on nst-1 as tim). Unattended, resumable.
#
# Why: SpaceTime stores the AIS dataset in true UTC; our systems stored it as Europe/Berlin wall-clock
# time (1 h behind in winter, 2 h in summer). The HDFS export crossing_points.parquet is UTC already.
# This script moves every AIS copy to UTC and rebuilds the trip summary tables from the result:
#
#   phase 0  preflight: tools, disk, conversion self-tests, baseline counts, export of the ambiguous
#            hour (2024-10-27 02:00-03:00 Berlin happens twice; those rows can't be converted from
#            wall-clock time, so they're taken from HDFS: UTC 2024-10-27 00:00-02:00)
#   phase 1  DuckDB      crossing_points → UTC (CTAS + swap), crossings_summary rebuilt
#   phase 2  ClickHouse  crossing_points reloaded from HDFS with timestamp DateTime('UTC') (staged,
#                        balanced over the 5 shards, swap), crossings_summary rebuilt as
#                        DateTime('UTC'), flights_summary rebuilt from flight_points
#   phase 3  Postgres    crossing_points → UTC month by month (staged, one transaction per month),
#                        ambiguous hour from HDFS, crossings_summary rebuilt (staged, swap)
#   phase 4  MobilityDB  crossings trajectories → true UTC instants, tile by tile (shiftTime); trips
#                        that span a DST change or touch the ambiguous hour are rebuilt from the
#                        (already converted) Postgres points
#   phase 5  HDFS        crossings_summary.parquet rebuilt with spark-sql from crossing_points.parquet
#   phase 6  verification report (sample trips must show the UTC times SpaceTime shows)
#
# Trip summaries (all systems) contain real trips only: 0 < id < 1e12 (SpaceTime's crossings: 2,018,173).
#
# Usage:
#   ./ais_utc_migration.sh --check        # preflight + statement self-tests only; changes no data (creates
#                                         # the empty helper tables and exports the ambiguous-hour CSV)
#   ./ais_utc_migration.sh                # run (or resume) every phase
#   ./ais_utc_migration.sh --from 3       # (re)start at phase 3; finished steps are still skipped
#
# Resumability: every step writes a marker to $STATE/<step>.done when it has committed; Postgres steps
# additionally commit their marker row in the same transaction as the data change
# (table utc_migration_log), so a crash never leaves a step half-applied and marked done.
# Every swap keeps nothing it doesn't need: old copies are dropped only after the new one verified.
# =====================================================================================================
set -euo pipefail

WORK=/home/tim/utc_migration
STATE=$WORK/state
LOG=$WORK/migration.log
AMB_CSV=$WORK/ambiguous_hour.csv            # id,ts(UTC),lat,lon -- read by psql \copy and DuckDB
MIN_FREE_GB=${MIN_FREE_GB:-100}
NODES=(nst-1 nst-2 nst-3 nst-4 nst-5)
export PGPASSWORD=${PGPASSWORD:-postgres}
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop YARN_CONF_DIR=/opt/hadoop/etc/hadoop
DUCKDB=/home/tim/.duckdb/cli/latest/duckdb
HDFS=/opt/hadoop/bin/hdfs
SPARK_SQL=/opt/spark/bin/spark-sql
HDFS_CP='hdfs://nst-1:9000/geobenchr/crossing_points.parquet/*.parquet'
AMB_BERLIN_LO='2024-10-27 02:00:00'; AMB_BERLIN_HI='2024-10-27 03:00:00'   # wall-clock, ambiguous
AMB_UTC_LO='2024-10-27 00:00:00';    AMB_UTC_HI='2024-10-27 02:00:00'      # the same rows, in UTC
REAL_ID='id > 0 AND id < 1000000000000'

CHECK=0; FROM=0
while [ $# -gt 0 ]; do
  case "$1" in
    --check) CHECK=1 ;;
    --from)  FROM=$2; shift ;;
    *) echo "unknown argument $1"; exit 2 ;;
  esac; shift
done
mkdir -p "$STATE"; chmod 755 "$WORK"

log()  { echo "$(date -Is) $*" | tee -a "$LOG"; }
die()  { log "FATAL: $*"; exit 1; }
done_() { [ -f "$STATE/$1.done" ]; }
mark() { date -Is > "$STATE/$1.done"; log "step $1 done"; }
step() { # step <name> <phase> -- true if the step should run now
  local name=$1 phase=$2
  [ "$phase" -lt "$FROM" ] && return 1
  if done_ "$name"; then log "step $name already done, skipping"; return 1; fi
  log "=== step $name"; return 0
}

pg()   { psql -h localhost -p 5433 -U postgres -d postgres -v ON_ERROR_STOP=1 -Atq "$@"; }
ch()   { clickhouse-client --port 9500 "$@"; }
chn()  { local n=$1; shift; if [ "$n" = nst-1 ]; then clickhouse-client --port 9500 "$@"; else ssh -o LogLevel=ERROR "$n" clickhouse-client --port 9500 "$(printf '%q ' "$@")"; fi; }
duck() { # duck <sql> -- runs one statement inside the quack server, prints CSV without header
  "$DUCKDB" -unsigned -csv -noheader -c \
    "LOAD quack; SELECT * FROM quack_query('quack:127.0.0.1:9494', \$q\$$1\$q\$, token := 'my-secret-token');"
}

disk_guard() {
  for n in "${NODES[@]}"; do
    local free
    if [ "$n" = nst-1 ]; then free=$(df -BG / | awk 'NR==2{print $4}' | tr -d G)
    else free=$(ssh -o LogLevel=ERROR "$n" "df -BG / | awk 'NR==2{print \$4}'" | tr -d G); fi
    [ "$free" -ge "$MIN_FREE_GB" ] || die "only ${free} GB free on $n (< ${MIN_FREE_GB} GB)"
  done
}

# ------------------------------------------------------------------------------------------------
# phase 0 -- preflight (always runs; --check stops after it)
# ------------------------------------------------------------------------------------------------
log "##### preflight (check=$CHECK, from=$FROM)"
for t in psql clickhouse-client "$DUCKDB" "$HDFS" "$SPARK_SQL"; do command -v "$t" >/dev/null || die "missing tool $t"; done
disk_guard
pg -c "CREATE TABLE IF NOT EXISTS utc_migration_log (step text PRIMARY KEY, done_at timestamptz DEFAULT now(), info text)"

# conversion self-tests: Berlin wall clock → UTC, values verified against SpaceTime/HDFS
want=$'2024-01-01 17:22:29\n2024-05-23 10:19:27'
got=$(pg -c "SELECT (ts AT TIME ZONE 'Europe/Berlin') AT TIME ZONE 'UTC' FROM (VALUES (TIMESTAMP '2024-01-01 18:22:29'), (TIMESTAMP '2024-05-23 12:19:27')) v(ts)")
[ "$got" = "$want" ] || die "Postgres conversion self-test: got [$got]"
got=$(duck "SELECT CAST((ts AT TIME ZONE 'Europe/Berlin') AT TIME ZONE 'UTC' AS VARCHAR) FROM (VALUES (TIMESTAMP '2024-01-01 18:22:29'), (TIMESTAMP '2024-05-23 12:19:27')) v(ts) ORDER BY 1")
[ "$got" = "$want" ] || die "DuckDB conversion self-test: got [$got]"
got=$(ch --query "SELECT toString(toDateTime(timestamp, 'UTC')) FROM hdfs('$HDFS_CP', 'Parquet') WHERE id IN (1010240101, 622240523) ORDER BY timestamp LIMIT 1 BY id FORMAT TSV")
[ "$got" = "$want" ] || die "HDFS UTC self-test (first point of the sample trips): got [$got]"
log "conversion self-tests OK"

# Citus planning self-tests on empty windows (no rows touched): staging insert, delete, MobilityDB update
pg -c "CREATE TABLE IF NOT EXISTS cp_utc_stage (LIKE crossing_points INCLUDING DEFAULTS)"
if [ "$(pg -c "SELECT count(*) FROM pg_dist_partition WHERE logicalrelid = 'cp_utc_stage'::regclass")" = 0 ]; then
  pg -c "SELECT create_distributed_table('cp_utc_stage', 'id', colocate_with => 'crossing_points')" >/dev/null
fi
pg <<SQL
BEGIN;
TRUNCATE cp_utc_stage;
INSERT INTO cp_utc_stage SELECT id, (timestamp AT TIME ZONE 'Europe/Berlin') AT TIME ZONE 'UTC', lat, lon, geom
  FROM crossing_points WHERE timestamp >= '1990-01-01' AND timestamp < '1990-02-01';
DELETE FROM crossing_points WHERE timestamp >= '1990-01-01' AND timestamp < '1990-02-01';
INSERT INTO crossing_points (id, timestamp, lat, lon, geom) SELECT id, timestamp, lat, lon, geom FROM cp_utc_stage;
TRUNCATE cp_utc_stage;
ROLLBACK;
SQL
pg -c "CREATE TABLE IF NOT EXISTS utc_mdb_rebuild_ids (id bigint PRIMARY KEY, tile_key int)"
if [ "$(pg -c "SELECT count(*) FROM pg_dist_partition WHERE logicalrelid = 'utc_mdb_rebuild_ids'::regclass")" = 0 ]; then
  pg -c "SELECT create_reference_table('utc_mdb_rebuild_ids')" >/dev/null
fi
pg <<SQL
BEGIN;
UPDATE crossings SET trip = shiftTime(trip, ((startTimestamp(trip) AT TIME ZONE 'UTC') AT TIME ZONE 'Europe/Berlin') - startTimestamp(trip))
 WHERE tile_key = -1 AND NOT EXISTS (SELECT 1 FROM utc_mdb_rebuild_ids r WHERE r.id = crossings.id);
ROLLBACK;
SQL
log "Citus planning self-tests OK"

# baselines (recorded once)
if [ ! -f "$STATE/baseline.txt" ]; then
  {
    echo "pg_points=$(pg -c "SELECT count(*) FROM crossing_points")"
    echo "ch_points=$(ch --query "SELECT count() FROM crossing_points")"
    echo "duck_points=$(duck "SELECT count(*) FROM crossing_points")"
    echo "pg_amb_berlin=$(pg -c "SELECT count(*) FROM crossing_points WHERE timestamp >= '$AMB_BERLIN_LO' AND timestamp < '$AMB_BERLIN_HI'")"
    echo "real_trips=$(pg -c "SELECT count(DISTINCT id) FROM crossing_points WHERE $REAL_ID")"
  } > "$STATE/baseline.tmp" && mv "$STATE/baseline.tmp" "$STATE/baseline.txt"
fi
# shellcheck disable=SC1091
source "$STATE/baseline.txt"
log "baseline: pg=$pg_points ch=$ch_points duck=$duck_points ambiguous-hour rows=$pg_amb_berlin real trips=$real_trips"
[ "$pg_points" = "$ch_points" ] && [ "$pg_points" = "$duck_points" ] || die "systems disagree on the row count before migrating"

# the ambiguous hour, exported once from HDFS (UTC); id=0 singletons get fresh ids >= 2e12
if [ ! -s "$AMB_CSV" ]; then
  ch --query "SELECT if(id = 0, 2000000000000 + rowNumberInAllBlocks(), id) AS id, toString(toDateTime(timestamp, 'UTC')) AS ts, lat, lon
              FROM hdfs('$HDFS_CP', 'Parquet')
              WHERE timestamp >= toDateTime64('$AMB_UTC_LO', 9, 'UTC') AND timestamp < toDateTime64('$AMB_UTC_HI', 9, 'UTC')
              FORMAT CSV" > "$AMB_CSV.tmp"
  mv "$AMB_CSV.tmp" "$AMB_CSV"; chmod 644 "$AMB_CSV"
fi
amb_rows=$(wc -l < "$AMB_CSV")
[ "$amb_rows" = "$pg_amb_berlin" ] || die "ambiguous hour: HDFS has $amb_rows rows, Postgres $pg_amb_berlin"
log "ambiguous hour: $amb_rows rows exported to $AMB_CSV"

if [ "$CHECK" = 1 ]; then log "##### --check finished, nothing changed"; exit 0; fi

# ------------------------------------------------------------------------------------------------
# phase 1 -- DuckDB
# ------------------------------------------------------------------------------------------------
if step duck_points 1; then
  disk_guard
  tables=$(duck "SELECT string_agg(table_name, ',') FROM information_schema.tables WHERE table_name IN ('crossing_points','crossing_points_utc')")
  if [[ ",$tables," != *",crossing_points,"* && ",$tables," == *",crossing_points_utc,"* ]]; then
    log "resuming after the drop: renaming crossing_points_utc"
  else
    duck "DROP TABLE IF EXISTS crossing_points_utc" >/dev/null
    duck "CREATE TABLE crossing_points_utc AS
            SELECT id, CAST((timestamp AT TIME ZONE 'Europe/Berlin') AT TIME ZONE 'UTC' AS TIMESTAMP) AS timestamp, lat, lon, geom
            FROM crossing_points
            WHERE NOT (timestamp >= TIMESTAMP '$AMB_BERLIN_LO' AND timestamp < TIMESTAMP '$AMB_BERLIN_HI')" >/dev/null
    duck "INSERT INTO crossing_points_utc
            SELECT id, CAST(ts AS TIMESTAMP), lat, lon, ST_Transform(ST_Point(lon, lat), 'EPSG:4326', 'EPSG:3857', true)
            FROM read_csv('$AMB_CSV', header = false, columns = {'id': 'BIGINT', 'ts': 'VARCHAR', 'lat': 'DOUBLE', 'lon': 'DOUBLE'})" >/dev/null
    n=$(duck "SELECT count(*) FROM crossing_points_utc")
    [ "$n" = "$duck_points" ] || die "DuckDB: crossing_points_utc has $n rows, expected $duck_points"
    s=$(duck "SELECT CAST(min(timestamp) AS VARCHAR) FROM crossing_points_utc WHERE id = 1010240101")
    [ "$s" = "2024-01-01 17:22:29" ] || die "DuckDB: sample trip starts at $s"
    duck "DROP TABLE crossing_points" >/dev/null
  fi
  duck "ALTER TABLE crossing_points_utc RENAME TO crossing_points" >/dev/null
  duck "CHECKPOINT" >/dev/null
  mark duck_points
fi
if step duck_summary 1; then
  duck "CREATE OR REPLACE TABLE crossings_summary AS
          SELECT id, min(timestamp) AS start_time, max(timestamp) AS end_time, count(*) AS num_points,
                 arg_min(geom, timestamp) AS start_location, arg_max(geom, timestamp) AS end_location
          FROM crossing_points WHERE $REAL_ID GROUP BY id" >/dev/null
  n=$(duck "SELECT count(*) FROM crossings_summary")
  [ "$n" = "$real_trips" ] || die "DuckDB crossings_summary: $n trips, expected $real_trips"
  duck "CHECKPOINT" >/dev/null
  mark duck_summary
fi

# ------------------------------------------------------------------------------------------------
# phase 2 -- ClickHouse
# ------------------------------------------------------------------------------------------------
if step ch_points 2; then
  disk_guard
  # resuming after the swap (e.g. the drop of the old copy failed): the live table is already UTC
  swapped=$(ch --query "SELECT countIf(type = 'DateTime(\\'UTC\\')') FROM clusterAllReplicas(cluster_1, system.columns) WHERE database = 'default' AND table = 'crossing_points_local' AND name = 'timestamp'")
  if [ "$swapped" = 5 ] && [ "$(ch --query "SELECT count() FROM crossing_points")" = "$ch_points" ] \
     && [ "$(ch --query "SELECT toString(min(timestamp)) FROM crossing_points WHERE id = 1010240101")" = "2024-01-01 17:22:29" ]; then
    log "ClickHouse: swap already done, dropping the leftover old copy"
    ch --query "DROP TABLE IF EXISTS crossing_points_local_v2 ON CLUSTER cluster_1 SYNC SETTINGS max_table_size_to_drop = 0" >/dev/null
    mark ch_points
  fi
fi
if step ch_points 2; then
  staged=$(ch --query "SELECT count() FROM clusterAllReplicas(cluster_1, system.tables) WHERE database = 'default' AND name = 'crossing_points_local_v2'")
  if [ "$staged" = 5 ] && [ "$(ch --query "SELECT count() FROM clusterAllReplicas(cluster_1, default.crossing_points_local_v2)")" = "$ch_points" ]; then
    log "ClickHouse: staged crossing_points_local_v2 already complete, reusing it"
  else
  ch --query "DROP TABLE IF EXISTS crossing_points_local_v2 ON CLUSTER cluster_1 SYNC SETTINGS max_table_size_to_drop = 0" >/dev/null
  ch --query "
    CREATE TABLE crossing_points_local_v2 ON CLUSTER cluster_1
    (
      id UInt64,
      timestamp DateTime('UTC'),
      lat Float64,
      lon Float64,
      Point Tuple(Float64, Float64) MATERIALIZED (lon, lat),
      h3 UInt64 MATERIALIZED geoToH3(lat, lon, 9),
      INDEX idx_lon lon TYPE minmax GRANULARITY 1,
      INDEX idx_lat lat TYPE minmax GRANULARITY 1,
      INDEX idx_ts timestamp TYPE minmax GRANULARITY 1
    )
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
    PARTITION BY toYYYYMM(timestamp)
    ORDER BY (h3, timestamp, id)
    SETTINGS index_granularity = 8192" >/dev/null
  # every node reads all of HDFS and keeps its 1/5 (balanced; the old load left 100 GB on nst-5);
  # id=0 singletons get per-node id blocks 1e12 + k*2e9 + n
  pids=()
  for k in 0 1 2 3 4; do
    n=${NODES[$k]}
    chn "$n" --query "
      INSERT INTO crossing_points_local_v2 (id, timestamp, lat, lon)
      SELECT if(id = 0, 1000000000000 + $k * 2000000000 + rowNumberInAllBlocks(), id), timestamp, lat, lon
      FROM hdfs('$HDFS_CP', 'Parquet')
      WHERE cityHash64(id, timestamp, lat, lon) % 5 = $k" > "$WORK/ch_load_$n.log" 2>&1 &
    pids+=($!)
  done
  for p in "${pids[@]}"; do wait "$p" || die "ClickHouse per-node load failed (see $WORK/ch_load_*.log)"; done
  fi
  n=$(ch --query "SELECT count() FROM clusterAllReplicas(cluster_1, default.crossing_points_local_v2)")
  [ "$n" = "$ch_points" ] || die "ClickHouse: staged table has $n rows, expected $ch_points"
  # remapped ids must be unique and inside each node's own block (1e12 + k*2e9 + n); checked per node
  # with a GROUP BY that may spill to disk (a cluster-wide uniqExact over ~2.2B ids exceeds the 9.3 GB
  # query memory limit; the numbering has gaps, so a max-min=count shortcut doesn't work either)
  for k in 0 1 2 3 4; do
    r=$(chn "${NODES[$k]}" --query "SELECT countIf(c > 1) = 0 AND min(id) >= 1000000000000 + $k * 2000000000 AND max(id) < 1000000000000 + ($k + 1) * 2000000000 FROM (SELECT id, count() AS c FROM default.crossing_points_local_v2 WHERE id >= 1000000000000 GROUP BY id) SETTINGS max_bytes_before_external_group_by = 3000000000, max_memory_usage = 9000000000")
    [ "$r" = 1 ] || die "ClickHouse: remapped ids on ${NODES[$k]} are not unique/in their block"
  done
  s=$(ch --query "SELECT toString(min(timestamp)) FROM clusterAllReplicas(cluster_1, default.crossing_points_local_v2) WHERE id = 1010240101")
  [ "$s" = "2024-01-01 17:22:29" ] || die "ClickHouse: sample trip starts at $s"
  ch --query "EXCHANGE TABLES crossing_points_local AND crossing_points_local_v2 ON CLUSTER cluster_1" >/dev/null
  ch --query "ALTER TABLE crossing_points ON CLUSTER cluster_1 MODIFY COLUMN timestamp DateTime('UTC')" >/dev/null
  ch --query "DROP TABLE crossing_points_local_v2 ON CLUSTER cluster_1 SYNC SETTINGS max_table_size_to_drop = 0" >/dev/null   # the old Berlin copy
  mark ch_points
fi
ch_summary() { # ch_summary <name> <points table> <ts type> <extra columns DDL> <extra select> <where>
  local name=$1 src=$2 tstype=$3 extra_ddl=$4 extra_sel=$5 where=$6
  ch --query "DROP TABLE IF EXISTS ${name}_v2_dist ON CLUSTER cluster_1 SYNC SETTINGS max_table_size_to_drop = 0" >/dev/null
  ch --query "DROP TABLE IF EXISTS ${name}_local_v2 ON CLUSTER cluster_1 SYNC SETTINGS max_table_size_to_drop = 0" >/dev/null
  ch --query "
    CREATE TABLE ${name}_local_v2 ON CLUSTER cluster_1
    (
      id UInt64, start_time $tstype, end_time $tstype, num_points UInt64, $extra_ddl
      start_lat Float64, start_lon Float64, end_lat Float64, end_lon Float64,
      start_point Tuple(Float64, Float64) MATERIALIZED (start_lon, start_lat),
      end_point Tuple(Float64, Float64) MATERIALIZED (end_lon, end_lat),
      start_h3 UInt64 MATERIALIZED geoToH3(start_lat, start_lon, 9)
    )
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
    ORDER BY (start_h3, start_time, id)
    SETTINGS index_granularity = 8192" >/dev/null
  ch --query "CREATE TABLE ${name}_v2_dist ON CLUSTER cluster_1 AS ${name}_local_v2
              ENGINE = Distributed('cluster_1', 'default', '${name}_local_v2', rand())" >/dev/null
  ch --query "
    INSERT INTO ${name}_v2_dist
    SELECT id, min(timestamp), max(timestamp), count(), $extra_sel
           argMin(lat, timestamp), argMin(lon, timestamp), argMax(lat, timestamp), argMax(lon, timestamp)
    FROM $src WHERE $where GROUP BY id
    SETTINGS distributed_foreground_insert = 1, parallel_distributed_insert_select = 0" >/dev/null
  # parallel_distributed_insert_select (default 2 here) would run the SELECT per shard and group only
  # that shard's points, duplicating every trip whose points span shards (9.1M rows for 2.0M trips)
}
if step ch_crossings_summary 2; then
  ch_summary crossings_summary crossing_points "DateTime('UTC')" "" "" "$REAL_ID"
  n=$(ch --query "SELECT count() FROM crossings_summary_v2_dist")
  [ "$n" = "$real_trips" ] || die "ClickHouse crossings_summary: $n trips, expected $real_trips"
  ch --query "EXCHANGE TABLES crossings_summary_local AND crossings_summary_local_v2 ON CLUSTER cluster_1" >/dev/null
  ch --query "ALTER TABLE crossings_summary ON CLUSTER cluster_1 MODIFY COLUMN start_time DateTime('UTC'), MODIFY COLUMN end_time DateTime('UTC')" >/dev/null
  ch --query "DROP TABLE crossings_summary_v2_dist ON CLUSTER cluster_1 SYNC SETTINGS max_table_size_to_drop = 0" >/dev/null
  ch --query "DROP TABLE crossings_summary_local_v2 ON CLUSTER cluster_1 SYNC SETTINGS max_table_size_to_drop = 0" >/dev/null
  mark ch_crossings_summary
fi
if step ch_flights_summary 2; then
  # aviation is not part of the UTC move; this only rebuilds the summary from the corrected
  # flight_points (it was built before the 2026-10-01 flight_points time fix: 146 trips off)
  ch_summary flights_summary flight_points "DateTime" "min_altitude Int64, max_altitude Int64," \
             "toInt64(min(altitude)), toInt64(max(altitude))," "1"
  n=$(ch --query "SELECT count() FROM flights_summary_v2_dist")
  want_f=$(pg -c "SELECT count(*) FROM flights_summary")
  [ "$n" = "$want_f" ] || die "ClickHouse flights_summary: $n trips, Postgres has $want_f"
  ch --query "EXCHANGE TABLES flights_summary_local AND flights_summary_local_v2 ON CLUSTER cluster_1" >/dev/null
  ch --query "DROP TABLE flights_summary_v2_dist ON CLUSTER cluster_1 SYNC SETTINGS max_table_size_to_drop = 0" >/dev/null
  ch --query "DROP TABLE flights_summary_local_v2 ON CLUSTER cluster_1 SYNC SETTINGS max_table_size_to_drop = 0" >/dev/null
  mark ch_flights_summary
fi

# ------------------------------------------------------------------------------------------------
# phase 3 -- Postgres crossing_points (+ crossings_summary)
# ------------------------------------------------------------------------------------------------
pg_logged() { # pg_logged <step> <sql...> -- data change and log row in one transaction
  local name=$1; shift
  [ "$(pg -c "SELECT count(*) FROM utc_migration_log WHERE step = '$name'")" = 1 ] && { log "step $name already committed"; return 0; }
  pg <<SQL
BEGIN;
$*
INSERT INTO utc_migration_log (step) VALUES ('$name');
COMMIT;
SQL
}
PG_MONTHS=(2024-01 2024-02 2024-03 2024-04 2024-05 2024-06 2024-07 2024-08 2024-09 2024-10 2024-11 2024-12
           2025-01 2025-02 2025-03 2025-04 2025-05 2025-06 2025-07)
# Each month's transaction leaves the old row versions and index entries behind as dead space that no
# later month reuses (converted months came out ~2x their size, ~17 GB per worker per month), so
# finished months are compacted with VACUUM FULL. A month is final once the month after it is done
# (rows shifted out of month M only land in M-1). This runs between months, never concurrently with
# a month's transaction: those lock every partition, and a waiting VACUUM FULL could deadlock them.
pg_compact_upto() { # pg_compact_upto <index>: compact every converted, not yet compacted month with index < $1
  local i P part
  for ((i = 0; i < $1; i++)); do
    P=${PG_MONTHS[$i]}; part="crossing_points_p${P/-/_}"
    done_ "compact_$P" && continue
    [ "$(pg -c "SELECT count(*) FROM utc_migration_log WHERE step = 'pg_month_$P'")" = 1 ] || continue
    disk_guard
    log "VACUUM FULL $part (before: $(pg -c "SELECT pg_size_pretty(citus_total_relation_size('$part'))"))"
    pg -c "VACUUM (FULL, ANALYZE) $part"
    log "compacted $part (after: $(pg -c "SELECT pg_size_pretty(citus_total_relation_size('$part'))"))"
    mark "compact_$P"
  done
}
if step pg_points 3; then
  # 1) drop the ambiguous Berlin hour (re-inserted from HDFS after the months are converted)
  pg_logged pg_amb_delete "DELETE FROM crossing_points WHERE timestamp >= '$AMB_BERLIN_LO' AND timestamp < '$AMB_BERLIN_HI';"
  # 2) months in ascending order: converted rows only ever move backwards in time, i.e. into a month
  #    that is already done, so nothing is converted twice
  for mi in "${!PG_MONTHS[@]}"; do
    m=${PG_MONTHS[$mi]}
    pg_compact_upto $((mi - 1))   # months before m-1 are final
    disk_guard
    lo="$m-01 00:00:00"; hi=$(date -d "$m-01 +1 month" +%Y-%m-01)" 00:00:00"
    log "postgres month $m"
    pg_logged "pg_month_$m" "
      TRUNCATE cp_utc_stage;
      INSERT INTO cp_utc_stage SELECT id, (timestamp AT TIME ZONE 'Europe/Berlin') AT TIME ZONE 'UTC', lat, lon, geom
        FROM crossing_points WHERE timestamp >= '$lo' AND timestamp < '$hi';
      DELETE FROM crossing_points WHERE timestamp >= '$lo' AND timestamp < '$hi';
      INSERT INTO crossing_points (id, timestamp, lat, lon, geom) SELECT id, timestamp, lat, lon, geom FROM cp_utc_stage;
      TRUNCATE cp_utc_stage;"
    part="crossing_points_p${m/-/_}"
    pg -c "VACUUM (ANALYZE) $part" || log "vacuum of $part failed (continuing)"
  done
  pg_compact_upto ${#PG_MONTHS[@]}   # the last months, now final too
  # 3) the ambiguous hour, from HDFS (already UTC)
  if [ "$(pg -c "SELECT count(*) FROM utc_migration_log WHERE step = 'pg_amb_insert'")" = 0 ]; then
    pg <<SQL
BEGIN;
CREATE TEMP TABLE amb (id bigint, ts timestamp, lat double precision, lon double precision) ON COMMIT DROP;
\copy amb FROM '$AMB_CSV' WITH (FORMAT csv)
INSERT INTO crossing_points (id, timestamp, lat, lon, geom)
  SELECT id, ts, lat, lon, ST_Transform(ST_SetSRID(ST_Point(lon, lat), 4326), 3857) FROM amb;
INSERT INTO utc_migration_log (step) VALUES ('pg_amb_insert');
COMMIT;
SQL
  fi
  n=$(pg -c "SELECT count(*) FROM crossing_points")
  [ "$n" = "$pg_points" ] || die "Postgres: $n rows after conversion, expected $pg_points"
  s=$(pg -c "SELECT min(timestamp) FROM crossing_points WHERE id = 1010240101")
  [ "$s" = "2024-01-01 17:22:29" ] || die "Postgres: sample trip starts at $s"
  pg -c "VACUUM (ANALYZE) crossing_points"
  mark pg_points
fi
if step pg_summary 3; then
  disk_guard
  pg <<SQL
DROP TABLE IF EXISTS crossings_summary_new;
CREATE TABLE crossings_summary_new (
  id bigint, start_time timestamp, end_time timestamp, num_points bigint,
  start_location geometry, end_location geometry);
SELECT create_distributed_table('crossings_summary_new', 'id', colocate_with => 'crossing_points');
INSERT INTO crossings_summary_new
  SELECT id, min(timestamp), max(timestamp), count(*),
         (array_agg(geom ORDER BY timestamp ASC))[1], (array_agg(geom ORDER BY timestamp DESC))[1]
  FROM crossing_points WHERE $REAL_ID GROUP BY id;
SQL
  n=$(pg -c "SELECT count(*) FROM crossings_summary_new")
  [ "$n" = "$real_trips" ] || die "Postgres crossings_summary_new: $n trips, expected $real_trips"
  pg <<SQL
CREATE UNIQUE INDEX ON crossings_summary_new (id);
CREATE INDEX ON crossings_summary_new USING gist (start_location);
CREATE INDEX ON crossings_summary_new USING gist (end_location);
CREATE INDEX ON crossings_summary_new (start_time);
BEGIN;
DROP TABLE crossings_summary;
ALTER TABLE crossings_summary_new RENAME TO crossings_summary;
COMMIT;
ANALYZE crossings_summary;
SQL
  mark pg_summary
fi

# ------------------------------------------------------------------------------------------------
# phase 4 -- MobilityDB crossings (needs phase 3: rebuilds read the converted Postgres points)
# ------------------------------------------------------------------------------------------------
# crossings stores the old wall-clock digits as UTC instants; true instant = (digits AT TIME ZONE
# Europe/Berlin), i.e. shift by -1 h / -2 h. Rows whose start and end need different shifts (a DST
# change inside the trip) or that touch the ambiguous hour can't be shifted as a whole: their trips
# are rebuilt from the converted points instead.
OFF="((%s AT TIME ZONE 'UTC') AT TIME ZONE 'Europe/Berlin') - %s"
off_start=$(printf "$OFF" "startTimestamp(trip)" "startTimestamp(trip)")
off_end=$(printf "$OFF" "endTimestamp(trip)" "endTimestamp(trip)")
if step mdb_collect 4; then
  pg_logged mdb_collect "
    TRUNCATE utc_mdb_rebuild_ids;
    INSERT INTO utc_mdb_rebuild_ids (id, tile_key)
      SELECT id, min(tile_key) FROM crossings
      WHERE ($off_start) <> ($off_end)
         OR trip && tstzspan '[$AMB_BERLIN_LO+00, $AMB_BERLIN_HI+00)'
      GROUP BY id;"
  log "trips to rebuild: $(pg -c "SELECT count(*) FROM utc_mdb_rebuild_ids")"
  mark mdb_collect
fi
# Shifting a tile rewrites every row of its shard, and plain VACUUM only makes the dead space reusable
# inside that shard, so an unchunked update doubles the shard on disk (nst-3 holds 8 tiles, ~230 GB).
# Each tile is therefore shifted in 4 chunks (id % 4) with a plain VACUUM of its shard in between, so
# later chunks reuse the space of earlier ones, and then compacted with VACUUM FULL on its worker.
# Peak extra space per tile is ~1.3x its compacted size instead of 2x, and no bloat is left behind.
pgw() { # pgw <node> <psql args...> -- run psql directly on a worker (single shard)
  local n=$1; shift
  ssh -o LogLevel=ERROR "$n" "PGPASSWORD=postgres psql -h localhost -p 5433 -U postgres -d postgres -v ON_ERROR_STOP=1 -Atq $(printf '%q ' "$@")"
}
tile_shard() { # tile_shard <tile> -> "<node> crossings_<shardid>"
  pg -c "SELECT n.nodename || ' crossings_' || s.shardid FROM pg_dist_shard s JOIN pg_dist_placement p USING (shardid)
         JOIN pg_dist_node n ON n.groupid = p.groupid
         WHERE s.logicalrelid = 'crossings'::regclass AND s.shardminvalue = '$1'"
}
tile_one() { # tile_one <tile> -- shift one tile in 4 chunks, then compact its shard
  local k=$1 node shard j
  read -r node shard <<< "$(tile_shard "$k")"
  [ -n "$shard" ] || die "no shard found for tile $k"
  if [ "$(pg -c "SELECT count(*) FROM utc_migration_log WHERE step = 'mdb_tile_$k'")" = 1 ]; then
    log "mobilitydb tile $k already shifted"
  else
    for j in 0 1 2 3; do
      disk_guard
      log "mobilitydb tile $k chunk $j/4 ($shard on $node)"
      pg_logged "mdb_tile_${k}_$j" "
        UPDATE crossings SET trip = shiftTime(trip, $off_start)
         WHERE tile_key = $k AND id % 4 = $j
           AND NOT EXISTS (SELECT 1 FROM utc_mdb_rebuild_ids r WHERE r.id = crossings.id);" || die "tile $k chunk $j failed"
      pgw "$node" -c "VACUUM $shard" || log "vacuum of $shard failed (continuing)"
    done
    pg_logged "mdb_tile_$k" "SELECT 1;" || die "tile $k log failed"
  fi
  if ! done_ "compact_tile_$k"; then
    log "VACUUM FULL $shard on $node (before: $(pgw "$node" -c "SELECT pg_size_pretty(pg_total_relation_size('$shard'))"))"
    pgw "$node" -c "VACUUM (FULL, ANALYZE) $shard" || die "VACUUM FULL $shard failed"
    log "compacted $shard (after: $(pgw "$node" -c "SELECT pg_size_pretty(pg_total_relation_size('$shard'))"))"
    mark "compact_tile_$k"
  fi
}
if step mdb_tiles 4; then
  # One lane per worker, in parallel: a tile's update and VACUUMs only touch the shard on its own
  # worker (single-shard router UPDATE, shard-local VACUUM), so lanes share no locks and no disk.
  # Within a lane tiles stay sequential, so each worker's peak disk use is the same as before.
  pids=()
  for n in $(pg -c "SELECT DISTINCT n.nodename FROM pg_dist_shard s JOIN pg_dist_placement p USING (shardid)
                    JOIN pg_dist_node n ON n.groupid = p.groupid WHERE s.logicalrelid = 'crossings'::regclass ORDER BY 1"); do
    (
      for k in $(pg -c "SELECT s.shardminvalue FROM pg_dist_shard s JOIN pg_dist_placement p USING (shardid)
                        JOIN pg_dist_node n ON n.groupid = p.groupid
                        WHERE s.logicalrelid = 'crossings'::regclass AND n.nodename = '$n' ORDER BY s.shardminvalue::int"); do
        tile_one "$k"
      done
      log "tile lane $n done"
    ) &
    pids+=($!)
  done
  failed=0
  for p in "${pids[@]}"; do wait "$p" || failed=1; done
  [ "$failed" = 0 ] || die "a tile lane failed (see above); finished tiles are logged, resume to continue"
  mark mdb_tiles
fi
if step mdb_rebuild 4; then
  pg_logged mdb_rebuild "
    DELETE FROM crossings WHERE id IN (SELECT id FROM utc_mdb_rebuild_ids);
    INSERT INTO crossings (id, tile_key, trip)
      SELECT p.id, r.tile_key,
             tgeompointseq(array_agg(tgeompoint(p.geom, p.timestamp AT TIME ZONE 'UTC') ORDER BY p.timestamp), 'discrete')
      FROM crossing_points p JOIN utc_mdb_rebuild_ids r ON r.id = p.id
      GROUP BY p.id, r.tile_key;
    -- the ambiguous hour's id=0 singletons got new ids (>= 2e12) in phase 3
    INSERT INTO crossings (id, tile_key, trip)
      SELECT id, 1, tgeompointseq(array_agg(tgeompoint(geom, timestamp AT TIME ZONE 'UTC') ORDER BY timestamp), 'discrete')
      FROM crossing_points WHERE id >= 2000000000000 GROUP BY id;"
  s=$(pg -c "SELECT min(startTimestamp(trip)) AT TIME ZONE 'UTC' FROM crossings WHERE id = 1010240101")
  [ "$s" = "2024-01-01 17:22:29" ] || die "MobilityDB: sample trip starts at $s"
  pg -c "VACUUM (ANALYZE) crossings"
  mark mdb_rebuild
fi

# ------------------------------------------------------------------------------------------------
# phase 5 -- HDFS crossings_summary.parquet (crossing_points.parquet is UTC already)
# ------------------------------------------------------------------------------------------------
if step hdfs_summary 5; then
  NEW=/geobenchr/crossings_summary.parquet.utc_new
  "$HDFS" dfs -rm -r -f -skipTrash "$NEW" >/dev/null 2>&1 || true
  "$SPARK_SQL" --master yarn --deploy-mode client \
    --conf spark.sql.session.timeZone=UTC --conf spark.sql.parquet.outputTimestampType=TIMESTAMP_MICROS -e "
    CREATE TEMPORARY VIEW cp USING parquet OPTIONS (path 'hdfs:///geobenchr/crossing_points.parquet');
    INSERT OVERWRITE DIRECTORY 'hdfs://$NEW' USING parquet
    SELECT id, min(timestamp) AS start_time, max(timestamp) AS end_time, count(*) AS num_points,
           min_by(geom, timestamp) AS start_location, max_by(geom, timestamp) AS end_location
    FROM cp WHERE $REAL_ID GROUP BY id;" >> "$WORK/spark_summary.log" 2>&1 || die "spark-sql failed (see $WORK/spark_summary.log)"
  n=$(ch --query "SELECT count() FROM hdfs('hdfs://nst-1:9000$NEW/*.parquet', 'Parquet')")
  [ "$n" = "$real_trips" ] || die "HDFS crossings_summary: $n trips, expected $real_trips"
  "$HDFS" dfs -rm -r -f -skipTrash /geobenchr/crossings_summary.parquet
  "$HDFS" dfs -mv "$NEW" /geobenchr/crossings_summary.parquet
  mark hdfs_summary
fi

# ------------------------------------------------------------------------------------------------
# phase 6 -- verification report
# ------------------------------------------------------------------------------------------------
log "##### verification (expected: 1010240101 starts 2024-01-01 17:22:29, 622240523 starts 2024-05-23 10:19:27 -- as on SpaceTime)"
log "postgres   points:   $(pg -c "SELECT count(*) FROM crossing_points") | $(pg -c "SELECT string_agg(id || ' ' || t, ' | ') FROM (SELECT id, min(timestamp) t FROM crossing_points WHERE id IN (1010240101, 622240523) GROUP BY id ORDER BY id) x")"
log "postgres   summary:  $(pg -c "SELECT count(*) FROM crossings_summary") | $(pg -c "SELECT string_agg(id || ' ' || start_time, ' | ') FROM crossings_summary WHERE id IN (1010240101, 622240523)")"
log "mobilitydb crossings: $(pg -c "SELECT string_agg(id || ' ' || t, ' | ') FROM (SELECT id, min(startTimestamp(trip)) AT TIME ZONE 'UTC' t FROM crossings WHERE id IN (1010240101, 622240523) GROUP BY id ORDER BY id) x")"
log "clickhouse points:   $(ch --query "SELECT count() FROM crossing_points") | $(ch --query "SELECT arrayStringConcat(groupArray(toString(id) || ' ' || toString(t)), ' | ') FROM (SELECT id, min(timestamp) t FROM crossing_points WHERE id IN (1010240101, 622240523) GROUP BY id ORDER BY id)")"
log "clickhouse summary:  $(ch --query "SELECT count() FROM crossings_summary") | flights_summary: $(ch --query "SELECT count() FROM flights_summary")"
log "duckdb     points:   $(duck "SELECT count(*) FROM crossing_points") | summary: $(duck "SELECT count(*) FROM crossings_summary")"
log "hdfs       summary:  $(ch --query "SELECT count() FROM hdfs('hdfs://nst-1:9000/geobenchr/crossings_summary.parquet/*.parquet', 'Parquet')")"
log "##### migration finished"
