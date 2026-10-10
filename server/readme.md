# Installation

The systems under test are installed on the five-node cluster with the ansible playbooks in
[`multinode/ansible/`](multinode/ansible/README.md) (PostGIS/Citus, MobilityDB, ClickHouse, DuckDB,
Hadoop/Spark/Sedona). `multinode/vm-setup/` creates test VMs, `multinode/data_processing/` and
`multinode/supporting_datasets/` prepare the data and reference datasets the playbooks load.

SpaceTime is proprietary software; for installation please contact the developing company
[Mireo](https://www.mireo.com/spacetime).

The single-node setups of the earlier paper (`postgis/`, `mobilitydb/`, `sedona/`, `spacetime/`,
`timescale/`) are kept locally in `single-node/paper/`, outside git.
