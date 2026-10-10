#!/bin/bash
# ClickHouse aviation: store flight_points / flights_summary timestamps as DateTime('UTC').
#
# Aviation times are UTC (Postgres stores them as naive UTC, HDFS as UTC instants). ClickHouse had them
# in plain DateTime columns, i.e. read as Europe/Berlin wall clock. That's harmless except for the
# spring-forward hour, which doesn't exist in Berlin: the 307,266 points of 2024-03-31 02:00-02:59
# were stored as 01:xx, mixed into the real 01:xx points (the data ends 2025-01-01, so that's the
# only such hour).
#
#   1. new flight_points_local_v2 (DateTime('UTC')) on every node: each node copies its own rows,
#      re-reading the Berlin wall clock as UTC (identical values), except 2024-03-31 01:00-03:59
#   2. those three hours are reloaded from HDFS (lat/lon = the same inverse Mercator as the original
#      load, bit-identical)
#   3. checks: total count, per-hour counts vs Postgres, and a hash over every untouched row
#   4. swap (EXCHANGE TABLES), Distributed table column type -> DateTime('UTC'); the old copy stays as
#      flight_points_local_v2 until the equivalence re-check passes
#   5. flights_summary rebuilt from the new flight_points as DateTime('UTC'), count vs Postgres, swap
set -euo pipefail
W=$HOME/pipeline/ch_aviation_utc
mkdir -p "$W"; LOG=$W/run.log
log() { echo "$(date -Is) $*" | tee -a "$LOG"; }
die() { log "FATAL: $*"; exit 1; }
ch()  { clickhouse-client --port 9500 "$@"; }
chn() { local n=$1; shift; if [ "$n" = nst-1 ]; then clickhouse-client --port 9500 "$@"; else ssh -o LogLevel=ERROR "$n" clickhouse-client --port 9500 "$(printf '%q ' "$@")"; fi; }
pg()  { PGPASSWORD=postgres psql -h localhost -p 5433 -U postgres -d postgres -v ON_ERROR_STOP=1 -Atq "$@"; }
NODES=(nst-1 nst-2 nst-3 nst-4 nst-5)
HDFS_FP="hdfs('hdfs://nst-1:9000/geobenchr/flight_points.parquet/*.parquet', 'Parquet')"
OLD_WIN="timestamp >= toDateTime('2024-03-31 01:00:00', 'Europe/Berlin') AND timestamp < toDateTime('2024-03-31 04:00:00', 'Europe/Berlin')"
NEW_WIN="timestamp >= toDateTime('2024-03-31 01:00:00', 'UTC') AND timestamp < toDateTime('2024-03-31 04:00:00', 'UTC')"

log "start"
total=$(ch --query "SELECT count() FROM flight_points")
[ "$total" = 2703631682 ] || die "flight_points has $total rows, expected 2703631682"
old_win=$(ch --query "SELECT count() FROM flight_points WHERE $OLD_WIN")
pg_win=$(pg -c "SELECT count(*) FROM flight_points WHERE timestamp >= '2024-03-31 01:00' AND timestamp < '2024-03-31 04:00'")
[ "$old_win" = "$pg_win" ] || die "window: ClickHouse $old_win rows vs Postgres $pg_win"
log "window 2024-03-31 01:00-04:00: $old_win rows (= Postgres)"

# 1) staged copy
ch --query "DROP TABLE IF EXISTS flight_points_local_v2 ON CLUSTER cluster_1 SYNC SETTINGS max_table_size_to_drop = 0" >/dev/null
ch --query "
  CREATE TABLE flight_points_local_v2 ON CLUSTER cluster_1
  (
    id UInt64,
    timestamp DateTime('UTC'),
    lat Float64,
    lon Float64,
    altitude Float64,
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
log "copying per node"
pids=()
for n in "${NODES[@]}"; do
  chn "$n" --query "
    INSERT INTO flight_points_local_v2 (id, timestamp, lat, lon, altitude)
    SELECT id, toDateTime(toString(timestamp), 'UTC'), lat, lon, altitude
    FROM flight_points_local WHERE NOT ($OLD_WIN)" > "$W/copy_$n.log" 2>&1 &
  pids+=($!)
done
for p in "${pids[@]}"; do wait "$p" || die "per-node copy failed (see $W/copy_*.log)"; done
log "copy done"

# 2) the three hours from HDFS (into nst-1's local table; ~930k rows)
ch --query "
  INSERT INTO flight_points_local_v2 (id, timestamp, lat, lon, altitude)
  SELECT id, toDateTime(timestamp, 'UTC'),
         degrees(2 * atan(exp(readWKBPoint(geom).2 / 6378137)) - pi() / 2),
         degrees(readWKBPoint(geom).1 / 6378137),
         altitude
  FROM $HDFS_FP
  WHERE timestamp >= toDateTime64('2024-03-31 01:00:00', 6, 'UTC') AND timestamp < toDateTime64('2024-03-31 04:00:00', 6, 'UTC')"
log "window reloaded from HDFS"

# 3) checks
V2="clusterAllReplicas(cluster_1, default.flight_points_local_v2)"
n=$(ch --query "SELECT count() FROM $V2")
[ "$n" = "$total" ] || die "staged table has $n rows, expected $total"
for h in 01 02 03; do
  a=$(ch --query "SELECT count() FROM $V2 WHERE timestamp >= toDateTime('2024-03-31 $h:00:00', 'UTC') AND timestamp < toDateTime('2024-03-31 $h:00:00', 'UTC') + 3600")
  b=$(pg -c "SELECT count(*) FROM flight_points WHERE timestamp >= '2024-03-31 $h:00' AND timestamp < '2024-03-31 $h:00'::timestamp + interval '1 hour'")
  [ "$a" = "$b" ] || die "hour $h: ClickHouse $a vs Postgres $b"
  log "hour $h: $a rows (= Postgres)"
done
h_old=$(ch --query "SELECT count(), sum(cityHash64(id, toString(timestamp), lat, lon, altitude)) FROM flight_points WHERE NOT ($OLD_WIN) FORMAT TSV")
h_new=$(ch --query "SELECT count(), sum(cityHash64(id, toString(timestamp), lat, lon, altitude)) FROM $V2 WHERE NOT ($NEW_WIN) FORMAT TSV")
[ "$h_old" = "$h_new" ] || die "untouched rows differ: old $h_old vs new $h_new"
log "untouched rows identical (count+hash $h_new)"
s_new=$(ch --query "SELECT toString(min(timestamp)) FROM $V2 WHERE id = 5391644")
s_pg=$(pg -c "SELECT min(timestamp) FROM flight_points WHERE id = 5391644")
[ "$s_new" = "$s_pg" ] || die "sample flight 5391644: ClickHouse $s_new vs Postgres $s_pg"

# 4) swap
ch --query "EXCHANGE TABLES flight_points_local AND flight_points_local_v2 ON CLUSTER cluster_1" >/dev/null
ch --query "ALTER TABLE flight_points ON CLUSTER cluster_1 MODIFY COLUMN timestamp DateTime('UTC')" >/dev/null
n=$(ch --query "SELECT count() FROM flight_points")
[ "$n" = "$total" ] || die "after swap flight_points has $n rows"
log "flight_points swapped (old Berlin copy kept as flight_points_local_v2)"

# 5) flights_summary
ch --query "DROP TABLE IF EXISTS flights_summary_v2_dist ON CLUSTER cluster_1 SYNC SETTINGS max_table_size_to_drop = 0" >/dev/null
ch --query "DROP TABLE IF EXISTS flights_summary_local_v2 ON CLUSTER cluster_1 SYNC SETTINGS max_table_size_to_drop = 0" >/dev/null
ch --query "
  CREATE TABLE flights_summary_local_v2 ON CLUSTER cluster_1
  (
    id UInt64, start_time DateTime('UTC'), end_time DateTime('UTC'), num_points UInt64,
    min_altitude Int64, max_altitude Int64,
    start_lat Float64, start_lon Float64, end_lat Float64, end_lon Float64,
    start_point Tuple(Float64, Float64) MATERIALIZED (start_lon, start_lat),
    end_point Tuple(Float64, Float64) MATERIALIZED (end_lon, end_lat),
    start_h3 UInt64 MATERIALIZED geoToH3(start_lat, start_lon, 9)
  )
  ENGINE = ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
  ORDER BY (start_h3, start_time, id)
  SETTINGS index_granularity = 8192" >/dev/null
ch --query "CREATE TABLE flights_summary_v2_dist ON CLUSTER cluster_1 AS flights_summary_local_v2
            ENGINE = Distributed('cluster_1', 'default', 'flights_summary_local_v2', rand())" >/dev/null
ch --query "
  INSERT INTO flights_summary_v2_dist
  SELECT id, min(timestamp), max(timestamp), count(), toInt64(min(altitude)), toInt64(max(altitude)),
         argMin(lat, timestamp), argMin(lon, timestamp), argMax(lat, timestamp), argMax(lon, timestamp)
  FROM flight_points GROUP BY id
  SETTINGS distributed_foreground_insert = 1, parallel_distributed_insert_select = 0" >/dev/null
n=$(ch --query "SELECT count() FROM flights_summary_v2_dist")
want=$(pg -c "SELECT count(*) FROM flights_summary")
[ "$n" = "$want" ] || die "flights_summary: $n trips, Postgres has $want"
d=$(pg -c "SELECT count(*) FROM flights_summary WHERE (start_time >= '2024-03-31 01:00' AND start_time < '2024-03-31 04:00') OR (end_time >= '2024-03-31 01:00' AND end_time < '2024-03-31 04:00')")
c=$(ch --query "SELECT count() FROM flights_summary_v2_dist WHERE ($(sed 's/timestamp/start_time/g' <<< "$NEW_WIN")) OR ($(sed 's/timestamp/end_time/g' <<< "$NEW_WIN"))")
[ "$c" = "$d" ] || die "flights_summary trips touching the window: ClickHouse $c vs Postgres $d"
ch --query "EXCHANGE TABLES flights_summary_local AND flights_summary_local_v2 ON CLUSTER cluster_1" >/dev/null
ch --query "ALTER TABLE flights_summary ON CLUSTER cluster_1 MODIFY COLUMN start_time DateTime('UTC'), MODIFY COLUMN end_time DateTime('UTC')" >/dev/null
ch --query "DROP TABLE flights_summary_v2_dist ON CLUSTER cluster_1 SYNC" >/dev/null
ch --query "DROP TABLE flights_summary_local_v2 ON CLUSTER cluster_1 SYNC SETTINGS max_table_size_to_drop = 0" >/dev/null   # the old Berlin summary
log "flights_summary rebuilt: $n trips (= Postgres), $c touching the window (= Postgres)"
log "DONE"
