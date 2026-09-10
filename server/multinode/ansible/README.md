# Installation of SUTs on machines
This ansible playbook installs the SUTs on a 5-node cluster. To do so, several configurations must be made by the benchmark user in order to guarantee a proper setup. 
Ansible needs to be installed along with Python on the machine from where the playbook is run, and an Ansible user needs to be configured on the machines where the system(s) need to be installed.

## Inventories

There are two separate inventories under `inventory/`, each a full `hosts.yml` +
`group_vars/all/all.yml` pair -- pick one with `-i` on every `ansible-playbook`/`ansible-inventory`
command below:

- **`inventory/local/`** -- disposable test VMs (see `../vm-setup/`), reached through a single
  floating IP with a per-node `ansible_port` (jump-host style). This is `ansible.cfg`'s default
  inventory, so a bare `ansible-playbook playbooks/...` with no `-i` targets this one -- a bare
  invocation can never accidentally hit production hardware.
- **`inventory/nst-cluster/`** -- the real 5-node SpaceTime cluster (`nst-1`..`nst-5`), owned by a
  partner and already running a live, proprietary SpaceTime installation (`nst-1` is the master).
  Access details (IPs, SSH user) are documented in
  `../../client/benchmark_client/docs/spacetime-cluster-access.md` (gitignored -- ask whoever set
  it up if you don't have it locally). Since these are someone else's production machines, its
  `group_vars/all/all.yml` sets `purge_conflicting_installs: false`, which skips the
  "uninstall/delete any existing PostgreSQL or ClickHouse install" cleanup steps in
  `roles/postgres` and `roles/clickhouse` -- those steps have no reason to think SpaceTime uses
  either, but there's no reason to bet someone else's production install on that assumption
  either. If our own previous install on `nst-cluster` needs a clean re-run, pass
  `-e purge_conflicting_installs=true` for that run only.

Each inventory's `hosts.yml` needs: one entry under `masters` (exactly one -- it acts as the Citus
coordinator and the single DuckDB/PostGIS host benchmark_client connects to) and the rest under
`workers`, each with `ansible_host` (+ `ansible_port` if you're going through a jump host/floating
IP with per-node SSH ports, as `inventory/local` does) for how Ansible reaches it, `ip` set to that
node's address on the network the *nodes themselves* use to talk to each other (this does not have
to be the same as `ansible_host` -- see the comment in `inventory/nst-cluster/hosts.yml` about the
one node where we know this differs), and a unique `num_id` (used for ClickHouse shard/replica/
keeper IDs). Nothing elsewhere needs to change -- every role derives node lists, hostnames, and IPs
from this file. Since the two inventories don't share a common vars file (see the comment at the
top of each `group_vars/all/all.yml`), a *shared* variable change needs to be made in both places.

Note also that `nst-cluster`'s nodes need `tim` to have (passwordless, or supply
`--ask-become-pass`) sudo rights for any of the `become: true` tasks below to work -- confirm this
before running anything wider than `--check`.

This playbook requires several files to be downloaded before installation. Currently, the playbook is setup to download the required files from the internet to the SUT machines. There is however also the possibility to upload them from the local machine (commented out in the relevant playbooks).  
To install all SUTs on the machine, please run the following: 
``` 
ansible-playbook -i inventory/local/hosts.yml playbooks/all.yml -v
```
(replace `inventory/local/hosts.yml` with `inventory/nst-cluster/hosts.yml` throughout this file to target the production cluster instead)

Just Sedona: 
```
ansible-playbook -i inventory/local/hosts.yml playbooks/hadoop.yml -v
ansible-playbook -i inventory/local/hosts.yml playbooks/spark.yml -v
ansible-playbook -i inventory/local/hosts.yml playbooks/sedona.yml -v
```

Just MobilityDB/PostGIS/Citus:
```
ansible-playbook -i inventory/local/hosts.yml playbooks/mobilitydb_cluster.yml -v
```
Or, without the (slower, built-from-source) MobilityDB extension layer -- just PostGIS + Citus:
```
ansible-playbook -i inventory/local/hosts.yml playbooks/postgis_cluster.yml -v
```

Just ClickHouse:
```
ansible-playbook -i inventory/local/hosts.yml playbooks/clickhouse_cluster.yml -v
```

Just DuckDB (installed on the master node only -- benchmark_client only ever connects to a
single DuckDB host):
```
ansible-playbook -i inventory/local/hosts.yml playbooks/duckdb.yml -v
```

`playbooks/all.yml` (the "install all SUTs" command above) runs all four of the stacks above,
back to back, across the whole cluster.

## Loading data
Installing the SUTs (above) only gets empty database systems running. Each real dataset
(`ais`, `aviation`, `cycling`, `geolife`) has its own pair of "load data" playbooks -- one per
dialect that actually stores bulk row data (PostGIS/Citus and ClickHouse; MobilityDB/Sedona/
DuckDB/SpaceTime all read the same PostGIS tables at query time, so they need no separate load
step):
```
ansible-playbook -i inventory/local/hosts.yml playbooks/insert_aviation_data.yml -v            # PostGIS
ansible-playbook -i inventory/local/hosts.yml playbooks/insert_aviation_data_clickhouse.yml -v  # ClickHouse
```
and likewise `insert_ais_data[_clickhouse].yml`, `insert_cycling_data[_clickhouse].yml`,
`insert_geolife_data[_clickhouse].yml`. Each one runs on `masters` only (there is exactly one
Postgres/Citus coordinator and one ClickHouse ingestion point per cluster) and:
1. Syncs a parquet file from the control machine to the target node -- `<dataset>_sample_parquet_file`
   in the chosen inventory's `group_vars/all/all.yml`. For `aviation` this is one real file out of the full
   `data_generator_aviation` output (the rest is tens of GB, too large to sync routinely); for
   `ais`/`cycling`/`geolife` it's a small synthetic dummy file matching that pipeline's real schema
   and configured partition time range, since the real generators for those either produce nothing
   checked in locally or are similarly too large. Point these variables at real generator output
   instead if you have it and want to load more than the sample.
2. Creates the dataset's table (Citus-distributed and time-partitioned for PostGIS; a `Distributed`
   engine table over per-shard `MergeTree` tables for ClickHouse) and loads every `*.parquet` file
   found in that dataset's data directory into it.
3. Builds the indexes queries need (GIST on geometry columns, plus btree on id/timestamp/etc.).

`playbooks/sync_supporting_datasets.yml` (role `add_supporting_datasets`) separately syncs the
small CSV reference datasets under `supporting_datasets/` (harbours, islands, regions -- used by
some `ais` queries) to every node, independent of the per-dataset parquet loads above.

`playbooks/stop_clickhouse.yml` stops the ClickHouse service cluster-wide -- useful before an
`insert_*_data_clickhouse.yml` rerun that needs to drop/recreate tables cleanly, or before shutting
the VMs down.

## Notes
- All cross-node references (Citus's `citus_add_node()`, Hadoop's `core-site.xml`/`workers`,
  ClickHouse's cluster/keeper config) use each node's bare inventory hostname (e.g.
  `geobenchr-sut-3`), not its IP -- so the `common` role writes `/etc/hosts` entries for every
  host in the inventory (derived from each host's `ip:` field) before anything else runs, so that
  name resolution works even if the target network has no DNS for these hostnames. If you rename
  hosts or change IPs in either inventory's `hosts.yml`, this is regenerated automatically on the
  next run -- nothing else needs to change.
- Similarly, `pg_hba.conf`'s Citus trust rules and the ClickHouse cluster/keeper templates are
  generated from the inventory (one rule/entry per host) rather than hardcoded, so a fresh
  inventory with different hostnames/IPs should work without editing any role or template.
- `local_parquet_dir` / `local_supporting_dataset_path` in each inventory's `group_vars/all/all.yml`
  are local paths on the machine you run `ansible-playbook` from (used as rsync/copy sources), not on
  the target servers -- they're defined relative to `playbook_dir` (Ansible's magic variable for
  "wherever the currently-running playbook lives"), so they resolve correctly no matter which
  machine you run `ansible-playbook` from, as long as this repo's own directory layout stays
  intact. No editing needed when switching control machines.
- Spark executor sizing (used by `roles/spark`'s dynamic allocation config and `roles/hadoop`'s
  YARN NodeManager capacity) is computed automatically from gathered facts in `roles/common`, not
  hand-tuned -- executor memory/cores are sized to fit the *smallest* worker in the chosen
  inventory's `hosts.yml` (so a heterogeneous cluster isn't overcommitted), and the min/max/initial
  executor counts are derived from how many of those fit across every worker. Resizing the cluster
  (adding workers, changing VM specs) just needs a rerun; nothing here needs manual adjustment.
