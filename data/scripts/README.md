# Data preparation and loading (single-node setup)

Scripts of the earlier single-node setup: they turn the raw data in `../raw/` into the formats the
systems load and load it into one server. The five-node cluster is loaded by the ansible roles in
[`../../server/multinode/ansible/`](../../server/multinode/ansible/README.md) instead.

| Script | Purpose |
|---|---|
| `prepareAISdata.py`, `prepareAviationData.py`, `prepareCyclingData.py` | merge and clean the raw CSV files of each scenario |
| `prepareCyclingDataForMobilityDB.py`, `prepareCyclingDataForSQLFromMobilityDBData.py` | cycling data as MobilityDB trajectories, and back to point rows |
| `unifyData.py`, `csvToGeoParquet.py`, `validateGeoParquet.py` | common schema, conversion to GeoParquet, a sanity check of the result |
| `loadResourcesOnServer.sh`, `transferData.sh` | copy the reference datasets and the prepared data to the server (`<ip> <user>`) |
| `loadData.sh`, `loadAllData.sh` | load a scenario into a system at a given size (`full`, `1M`, `10M`, `100M`, ...) |
| `createGraph.ipynb`, `visualize.ipynb` | plots of the data |
| `trajectory.pdf`, `trajectory_representation_options.pdf` | notes on how trajectories are represented |

Usage is described in [`../readme.md`](../readme.md).
