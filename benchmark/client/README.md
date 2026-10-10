# Benchmark client

[`benchmark_client/`](benchmark_client/) is the Java (17, Maven) client that runs the benchmark queries
against every system and logs each execution's time and outcome.

| Path | Content |
|---|---|
| `benchmark_client/src/main/java/benchmark_client/` | `BenchmarkClient` (entry point), `BenchmarkRunner` (repetitions, timeouts), one executor per system (`PostgreSQLExecutor` for PostGIS and MobilityDB, `ClickHouseExecutor`, `DuckDBExecutor`, `SpaceTimeExecutor`, `SedonaExecutor`), `JdbcSessions` (reused sessions), `QueryLogger` |
| `benchmark_client/src/main/resources/<scenario>/` | the generated queries per system (`<dialect>_queries.yaml`), written by `benchmark/configuration/<scenario>/scripts/generateParameterVariations.py` |
| `benchmark_client/scripts/` | `spacetime_query_runner.py` (Python bridge to SpaceTime, which has no JDBC driver), `reset_benchmark_writes.sh` (removes rows written by write queries), data fixes for `crossing_points` |
| `benchmark_client/run_scenario_check.sh`, `run_sedona_check_nst1.sh` | functionality checks: run each query once per system |
| `benchmark_client/docs/` | the [client documentation](benchmark_client/docs/README.md): building, running, environment variables, what a timing measures, output format |

The setup for full, isolated benchmark runs (one system at a time, hardware sampling) is in
[`../configuration/scripts/realbench/`](../configuration/scripts/realbench/).
