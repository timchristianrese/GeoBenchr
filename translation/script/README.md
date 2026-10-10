# Query generators

The code of the YAML-to-query translation (see [`../README.md`](../README.md) for the YAML format).

| File | Content |
|---|---|
| `generateQueries.py` | command line entry point: reads YAML files and writes each query for the selected systems |
| `queryConfigGenerator.py` | parses the YAML into a `QueryConfig` tree (sources, filters, joins, aggregations, ...) |
| `postGISGenerator.py`, `sedonaGenerator.py`, `spaceTimeGenerator.py`, `CQLGenerator.py`, `mongoDBGenerator.py` | render a `QueryConfig` as PostGIS SQL, Sedona (PySpark), SpaceTime SQL, CQL and a MongoDB aggregation pipeline |
| `queries/` | generated output |
