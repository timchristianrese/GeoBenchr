#!/bin/bash
# Memory settings at recommended production values (2026-10-14), run on nst-1. Nodes: 62 GB RAM, 20 cores.
#   ClickHouse: no per-query limit of its own (was max_memory_usage = 10 GB in users.d/users.xml); the server
#               cap (max_server_memory_usage_to_ram_ratio 0.9, the default) bounds every query. svc.sh now
#               restarts the system under test after stopping the others, so ClickHouse computes that cap
#               on an otherwise idle node.
#   YARN/Spark: the NodeManagers hand out 54 GB per node (was all 62 GB: nothing kept for the OS, the DataNode
#               and the page cache) and 20 vcores, so Sedona runs 4 executors per node with 5 cores and
#               11 GB (+1.1 GB overhead) each -- 20 executors, 100 cores -- instead of 90 executors with
#               1 core and 2 GB.
#   Postgres (16 GB shared_buffers, 46 GB effective_cache_size, 2 GB maintenance_work_mem) and DuckDB
#   (memory_limit 80 % of RAM, 20 threads) are at their recommended values already.
# Every changed file is backed up as <file>.bak_20261014.
set -euo pipefail
NODES=(localhost nst-2 nst-3 nst-4 nst-5)
HERE=$(cd "$(dirname "$0")" && pwd)

# ClickHouse
for h in "${NODES[@]}"; do
  ssh "$h" "sudo cp -n /etc/clickhouse-server/users.d/users.xml /etc/clickhouse-server/users.d/users.xml.bak_20261014
            sudo sed -i 's#<max_memory_usage>10000000000</max_memory_usage>#<max_memory_usage>0</max_memory_usage>#' /etc/clickhouse-server/users.d/users.xml
            sudo grep -q '<max_memory_usage>0</max_memory_usage>' /etc/clickhouse-server/users.d/users.xml" || { echo "clickhouse users.xml not changed on $h"; exit 1; }
done
echo "clickhouse: max_memory_usage 0 on all nodes (takes effect on the next restart by svc.sh)"

# YARN
Y=/opt/hadoop/etc/hadoop/yarn-site.xml
for h in "${NODES[@]}"; do
  ssh "$h" "cp -n $Y $Y.bak_20261014 && python3 - <<'PY'
import re
p = '$Y'; s = open(p).read()
want = {'yarn.nodemanager.resource.memory-mb': '55296', 'yarn.scheduler.maximum-allocation-mb': '55296',
        'yarn.nodemanager.resource.cpu-vcores': '20', 'yarn.scheduler.maximum-allocation-vcores': '20'}
for k, v in want.items():
    pat = re.compile(r'(<name>' + re.escape(k) + r'</name>\s*<value>)[^<]*(</value>)')
    assert pat.search(s), k
    s = pat.sub(r'\g<1>' + v + r'\g<2>', s)
open(p, 'w').write(s)
PY
grep -A1 -E 'resource.memory-mb|maximum-allocation-mb|resource.cpu-vcores|maximum-allocation-vcores' $Y | grep value | tr -d ' ' | tr '\n' ' '; echo"
done

# svc.sh with the restart of the system under test
[ "$HERE/svc.sh" -ef "$HOME/realbench/svc.sh" ] || cp "$HERE/svc.sh" "$HOME/realbench/svc.sh"

# restart YARN (ResourceManager + NodeManagers) and ClickHouse with the new settings, then everything up
"$HOME/realbench/svc.sh" only Sedona
"$HOME/realbench/svc.sh" only ClickHouse
"$HOME/realbench/svc.sh" all
curl -s http://nst-1:8088/ws/v1/cluster/metrics | python3 -c "import json,sys; m=json.load(sys.stdin)['clusterMetrics']; print('yarn:', m['activeNodes'], 'nodes,', m['totalMB'] // 1024, 'GB,', m['totalVirtualCores'], 'vcores')"
clickhouse-client --port 9500 --query "SELECT hostName(), getSetting('max_memory_usage') FROM clusterAllReplicas(cluster_1, system.one) ORDER BY 1"
