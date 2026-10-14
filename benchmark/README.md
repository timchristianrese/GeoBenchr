# Benchmark

The benchmark client, the query configuration it reads, and the results.

| Folder | Content |
|---|---|
| [`client/`](client/README.md) | the Java benchmark client: one executor per system, repetitions, timeouts, logging ([full documentation](client/benchmark_client/docs/README.md)) |
| [`configuration/`](configuration/README.md) | the queries of each scenario in every system's dialect (`<scenario>/config/combinedBenchConf.yaml`), the generators that draw their parameters, and the scripts used to prepare the systems and run the benchmark (`scripts/`; the isolated benchmark runs are in `scripts/realbench/`) |
| [`results/`](results/README.md) | methodology and results: `build_results.py` turns the raw client logs, query files and hardware samples into CSV tables and plots; `analysis.ipynb` explores them |
| `sedonadb.ipynb` | SedonaDB (separate from Apache Sedona): data loading and benchmark in one notebook |

A run in short: generate the queries (`configuration/<scenario>/scripts/generateParameterVariations.py`),
which copies them into the client's resources; build and run the client; turn its logs into results
with `results/build_results.py`.
