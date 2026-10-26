#!/bin/bash
# Service control for the isolated benchmark (2026-10-09), run on nst-1 (as tim; systemctl via sudo).
#   svc.sh only <PostGIS|MobilityDB|ClickHouse|DuckDB|Sedona|SpaceTime>
#       stops every other benchmarked system on nst-1..5, starts the one under test, waits until healthy
#   svc.sh all        starts everything again (end of the benchmark)
#   svc.sh status     one line per system
#   svc.sh restart-duckdb   restart the DuckDB server and wait until it answers -- the only way to stop a
#                     running DuckDB query (the benchmark client's DUCKDB_CANCEL_CMD on a timeout)
# SpaceTime (spacetimed, run by the partner as user stdb) is never stopped. MinIO (not used by any
# benchmarked system) is stopped for "only" and started again by "all".
set -u
export JAVA_HOME=/opt/java/latest HADOOP_HOME=/opt/hadoop HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
NODES=(nst-1 nst-2 nst-3 nst-4 nst-5)
WORKERS=(nst-2 nst-3 nst-4 nst-5)
log() { echo "$(date -Is) [svc] $*"; }
on() { local n=$1; shift; if [ "$n" = nst-1 ]; then bash -c "$*"; else ssh -o LogLevel=ERROR "$n" "$*"; fi; }

pg_stop()   { for n in nst-1 "${WORKERS[@]}"; do on $n "sudo systemctl stop postgresql@17-main"; done; }
pg_start()  { for n in "${WORKERS[@]}" nst-1; do on $n "sudo systemctl start postgresql@17-main"; done
              for i in $(seq 1 60); do
                w=$(PGPASSWORD=postgres psql -h localhost -p 5433 -U postgres -Atc "SELECT count(*) FROM citus_get_active_worker_nodes()" 2>/dev/null)
                [ "$w" = 4 ] && return 0; sleep 5; done; return 1; }
ch_stop()   { for n in "${NODES[@]}"; do on $n "sudo systemctl stop clickhouse-server; sudo systemctl stop clickhouse-keeper"; done; }
ch_start()  { for n in "${NODES[@]}"; do on $n "sudo systemctl start clickhouse-keeper"; done
              for n in "${NODES[@]}"; do on $n "sudo systemctl start clickhouse-server"; done
              for i in $(seq 1 60); do
                c=$(clickhouse-client --port 9500 --query "SELECT count() FROM clusterAllReplicas(cluster_1, system.one)" 2>/dev/null)
                [ "$c" = 5 ] && break; sleep 5; done
              [ "$c" = 5 ] || return 1
              # (2026-10-26) the servers answer before they have loaded their tables (asynchronous loading):
              # a write right after the start failed with "Table is not initialized yet". Wait until every
              # replica of every point table is loaded and writable.
              for i in $(seq 1 120); do
                r=$(clickhouse-client --port 9500 --query "SELECT countIf(NOT is_readonly AND NOT is_session_expired) FROM clusterAllReplicas(cluster_1, system.replicas) WHERE table LIKE '%_points_local'" 2>/dev/null)
                l=$(clickhouse-client --port 9500 --query "SELECT count() FROM clusterAllReplicas(cluster_1, system.asynchronous_loader) WHERE status != 'OK'" 2>/dev/null)
                [ "$r" = 20 ] && [ "$l" = 0 ] && return 0; sleep 5; done; return 1; }
duck_stop() { sudo systemctl stop duckdb-quack; }
duck_start(){ sudo systemctl start duckdb-quack
              for i in $(seq 1 60); do
                r=$(/home/tim/.duckdb/cli/latest/duckdb -unsigned -csv -noheader -c "LOAD quack; SELECT * FROM quack_query('quack:127.0.0.1:9494', \$q\$SELECT 42\$q\$, token := 'my-secret-token');" 2>/dev/null)
                [ "$r" = 42 ] && return 0; sleep 5; done; return 1; }
# Hadoop daemons run as tim, not under systemd; sbin/stop-*.sh rely on pid files these daemons don't
# have, so they are stopped by process name (jps) and started per daemon.
HENV="export JAVA_HOME=/opt/java/latest HADOOP_HOME=/opt/hadoop HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop;"
hdp_kill()  { on $1 "for p in \$(/opt/java/latest/bin/jps | awk '/^[0-9]+ ($2)\$/{print \$1}'); do kill \$p; done"; }
hdp_stop()  { for n in "${NODES[@]}"; do hdp_kill $n "NodeManager|DataNode"; done
              hdp_kill nst-1 "ResourceManager|SecondaryNameNode|NameNode"
              for i in $(seq 1 30); do
                left=0; for n in "${NODES[@]}"; do c=$(on $n "/opt/java/latest/bin/jps | grep -cE 'NameNode|DataNode|ResourceManager|NodeManager'"); left=$((left + c)); done
                [ "$left" = 0 ] && return 0; sleep 3; done; return 1; }
hdp_start() { on nst-1 "$HENV /opt/hadoop/bin/hdfs --daemon start namenode; /opt/hadoop/bin/hdfs --daemon start secondarynamenode"
              for n in "${WORKERS[@]}"; do on $n "$HENV /opt/hadoop/bin/hdfs --daemon start datanode"; done
              /opt/hadoop/bin/hdfs dfsadmin -safemode wait >/dev/null 2>&1
              on nst-1 "$HENV /opt/hadoop/bin/yarn --daemon start resourcemanager"
              for n in "${NODES[@]}"; do on $n "$HENV /opt/hadoop/bin/yarn --daemon start nodemanager"; done
              for i in $(seq 1 60); do
                r=$(curl -s "http://nst-1:8088/ws/v1/cluster/metrics" | python3 -c "import json,sys; print(json.load(sys.stdin)['clusterMetrics']['activeNodes'])" 2>/dev/null)
                [ -n "$r" ] && [ "$r" -ge 3 ] && return 0; sleep 5; done; return 1; }
minio()     { for n in "${NODES[@]}"; do on $n "sudo systemctl $1 minio"; done; }

status() {
  echo "postgres:   $(for n in "${NODES[@]}"; do echo -n "$(on $n 'systemctl is-active postgresql@17-main') "; done)"
  echo "clickhouse: $(for n in "${NODES[@]}"; do echo -n "$(on $n 'systemctl is-active clickhouse-server') "; done)"
  echo "duckdb:     $(systemctl is-active duckdb-quack)"
  echo "hadoop:     $(/opt/java/latest/bin/jps 2>/dev/null | grep -cE 'NameNode|ResourceManager') master daemons on nst-1; yarn nodes: $(curl -s -m 3 http://nst-1:8088/ws/v1/cluster/metrics 2>/dev/null | python3 -c "import json,sys; print(json.load(sys.stdin)['clusterMetrics']['activeNodes'])" 2>/dev/null || echo down)"
  echo "minio:      $(for n in "${NODES[@]}"; do echo -n "$(on $n 'systemctl is-active minio') "; done)"
  echo "spacetime:  $(for n in "${NODES[@]}"; do echo -n "$(on $n 'pgrep -x spacetimed >/dev/null && echo up || echo down') "; done)"
}

case "${1:-status}" in
  only)
    sys=$2
    log "isolating $sys"
    minio stop
    [ "$sys" = PostGIS ] || [ "$sys" = MobilityDB ] || pg_stop
    [ "$sys" = ClickHouse ] || ch_stop
    [ "$sys" = DuckDB ] || duck_stop
    [ "$sys" = Sedona ] || hdp_stop
    # (2026-10-14) the system under test is restarted once the others are stopped, so it starts on otherwise
    # idle nodes: ClickHouse sizes its server memory cap (90 % of RAM) from the memory available at start-up,
    # and kept a cap computed while every other system was running.
    case "$sys" in
      PostGIS|MobilityDB) pg_stop; pg_start || { log "FATAL: postgres not healthy"; exit 1; } ;;
      ClickHouse) ch_stop; ch_start || { log "FATAL: clickhouse not healthy"; exit 1; }
                  log "clickhouse max_server_memory_usage: $(clickhouse-client --port 9500 --query "SELECT formatReadableSize(toUInt64(value)) FROM system.server_settings WHERE name = 'max_server_memory_usage'")" ;;
      DuckDB) duck_stop; duck_start || { log "FATAL: duckdb not healthy"; exit 1; } ;;
      Sedona) hdp_stop; hdp_start || { log "FATAL: hadoop/yarn not healthy"; exit 1; } ;;
      SpaceTime) : ;;
      *) log "FATAL: unknown system $sys"; exit 1 ;;
    esac
    log "$sys isolated"; status ;;
  all)
    log "starting everything"
    minio start; pg_start; ch_start; duck_start; hdp_start
    status ;;
  restart-duckdb)
    log "restarting duckdb (query timeout)"
    duck_stop; duck_start || { log "FATAL: duckdb not healthy"; exit 1; }
    log "duckdb back" ;;
  status) status ;;
esac
