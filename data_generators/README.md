# Data generators

One folder per scenario; each generates (or converts) the point data the benchmark loads. Setup and
usage are in each folder's `readme.md`.

| Folder | Scenario | Output (large outputs are git-ignored; small `dummy_sample.parquet` files stay tracked) |
|---|---|---|
| [`ais/`](ais/readme.md) | AIS maritime crossings | `output_data/crossing_points/` |
| [`aviation/`](aviation/readme.md) | flights (LSTM-generated trajectories) | `parquet_flights/` (676 files) |
| [`cycling/`](cycling/readme.md) | Berlin bicycle rides | `output/parquet/` (samples synced by ansible), `output/trajectories_full/` (full dataset) |
| [`geolife/`](geolife/generate_data.py) | synthetic activity points drawn from Geolife stay-point distributions (settings at the top of `generate_data.py`) | `synthetic_output/` |

The ansible inventories (`server/multinode/ansible/inventory/*/group_vars/all/all.yml`) read the
sample files from these paths.
