#!/bin/bash
# Same Postgres settings on the coordinator and every Citus worker (2026-10-13), run on nst-1.
# nst-3's worker never got the tuning block the other four nodes received on 2026-09-16 and ran with
# Postgres' defaults (128 MB shared_buffers, 4 MB work_mem, 2 parallel workers per gather). This
# appends that block where it is missing, adds two settings for the NVMe SSDs on every node, and
# restarts Postgres on all five nodes. postgresql.conf is backed up first (postgresql.conf.bak_20261013).
set -euo pipefail
CONF=/etc/postgresql/17/main/postgresql.conf
TUNING='
# FIX: applied for GeoBenchr'"'"'s partition-heavy tables and 20-core nodes (same on every node).
max_locks_per_transaction = 4096
max_connections = 300
max_worker_processes = 24
max_parallel_workers = 20
max_parallel_workers_per_gather = 6
shared_buffers = '"'"'16GB'"'"'
effective_cache_size = '"'"'46GB'"'"'
work_mem = '"'"'256MB'"'"'
maintenance_work_mem = '"'"'2GB'"'"''
SSD='
# (2026-10-13) NVMe SSDs on every node: random reads cost about as much as sequential ones.
random_page_cost = 1.1
effective_io_concurrency = 200'
for h in localhost nst-2 nst-3 nst-4 nst-5; do
  ssh "$h" "sudo cp -n $CONF $CONF.bak_20261013
    grep -q '^max_locks_per_transaction = 4096' $CONF || printf '%s\n' \"$TUNING\" | sudo tee -a $CONF >/dev/null
    grep -q '^random_page_cost = 1.1' $CONF || printf '%s\n' \"$SSD\" | sudo tee -a $CONF >/dev/null"
done
# workers first, coordinator last
for h in nst-2 nst-3 nst-4 nst-5 localhost; do ssh "$h" "sudo systemctl restart postgresql@17-main"; done
cat > /tmp/pg_check.sql <<'SQL'
select string_agg(name || '=' || setting || coalesce(unit, ''), ' ' order by name) from pg_settings
where name in ('shared_buffers','work_mem','effective_cache_size','max_parallel_workers_per_gather','max_parallel_workers','random_page_cost','effective_io_concurrency','maintenance_work_mem');
SQL
for h in localhost nst-2 nst-3 nst-4 nst-5; do
  scp -q /tmp/pg_check.sql "$h":/tmp/pg_check.sql
  echo "$h: $(ssh "$h" "sudo -u postgres psql -p 5433 -tA -f /tmp/pg_check.sql")"
done
