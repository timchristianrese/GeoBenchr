# Five-node cluster setup

| Folder | Content |
|---|---|
| [`ansible/`](ansible/README.md) | playbooks and roles that install and load every system on the five nodes: PostgreSQL/Citus with PostGIS and MobilityDB, ClickHouse (with Keeper), DuckDB (quack server), Hadoop/HDFS/YARN with Spark and Sedona, MinIO; inventories for the `nst-cluster`, local test VMs (`local`) and `exp1` |
| [`vm-setup/`](vm-setup/README.md) | creates (and removes) five local Debian VMs with KVM/libvirt for testing the playbooks |
| `data_processing/` | AIS data: `zip_to_gz.py` downloads the monthly archives of the [Danish Maritime Authority](http://aisdata.ais.dk) and repacks them as `csv.gz`, `csv_to_parquet.py` turns them into the `crossing_points` and `crossings` Parquet files (with trip ids); `data/`, `tmp/` and `output/` are git-ignored |
| `supporting_datasets/` | the reference tables every scenario joins with (airports, cities, counties, Berlin districts, universities, harbours, islands and coastlines) as CSV/Parquet, and the scripts that converted them; `resources/` holds the original CSV versions |

SpaceTime is installed and run by its developers; see [`../README.md`](../README.md).
