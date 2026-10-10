# Raw data samples

Small samples of the raw input data, one zip per scenario, from the earlier single-node setup. The
full raw data is not in the repository (tens of GB); the larger local copies here are git-ignored.

| Folder | Content |
|---|---|
| `ais/` | `sample.zip`: AIS positions from the [Piraeus AIS dataset](https://www.bisec.gr/dataset/the-piraeus-ais-dataset-for-large-scale-maritime-data-analytics) (described in [`ais/README.md`](ais/README.md)) |
| `aviation/` | `sample.zip`: flight positions (DFS format); the full data is available for research from the Deutsche Flugsicherung |
| `cycling/` | `sample.zip`: bicycle rides from [SimRa](https://github.com/simra-project/dataset) |

The five-node benchmark's data comes from [`../../data_generators/`](../../data_generators/README.md)
and, for AIS, from [`../../server/multinode/data_processing/`](../../server/multinode/README.md).
