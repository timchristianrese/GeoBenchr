# benchmark_client documentation

Central documentation for `benchmark_client`, the Java benchmark client. This
assumes the `server` and `data` installation steps (see the root
[README.md](../../../../README.md)) are already done.

- [Installation and running](#installation) — build the client and run a
  single-client benchmark against every system, including SpaceTime.
- [Environment variables](#environment-variables) — how to point the client at
  a different cluster, tune threads, etc. without editing source.
- [Choosing which systems run](#choosing-which-systems-run) — enabling/
  disabling individual SUTs.
- [Running against SpaceTime](#running-against-spacetime) — the one SUT that
  needs an extra prerequisite (no JDBC driver exists for it).
- [Output](#output) — where results and logs land, and what they look like.
- [SedonaDB vs. Sedona](#sedonadb-vs-sedona) — two different systems with
  similar names; only one is driven by this client.
- [Isolated benchmark runs](#isolated-benchmark-runs) — the setup of the 52-repetition
  run: one system at a time, everything else stopped, hardware sampled.
- [Query configuration](../../../configuration/README.md) — how the query files
  the client reads (`combinedBenchConf.yaml`, `generateParameterVariations.py`,
  etc.) are defined and generated; lives in `benchmark/configuration/` since it
  isn't specific to the Java client.
- [SpaceTime cluster access](spacetime-cluster-access.md) — internal, gitignored
  notes for connecting to a partner-hosted SpaceTime cluster (credentials/IPs;
  not for publication).
- [Sample queries per SUT](sample-queries.md) — internal, gitignored small
  smoke-test queries (verified working, with known issues called out) for each
  installed database system, independent of the full benchmark harness.

## Installation

The benchmark client is split into the `client` itself and necessary
`configuration` files. This installation assumes that you have already set up
the database systems which you would like to benchmark (`server` and `data`
steps from the root [README.md](../../../../README.md)).

The client runs every enabled system **sequentially, in one JVM process**,
against one scenario at a time: SpaceTime, PostGIS, MobilityDB, DuckDB,
ClickHouse, then Sedona (Apache Sedona, not SedonaDB — see
[SedonaDB vs. Sedona](#sedonadb-vs-sedona)).

1. **Generate the queries.** Check the configuration files found in scenario
   folders under `benchmark/configuration` to adjust the queries, repetitions,
   and which queries you want executed. Each query in `combinedBenchConf` can
   be toggled to `true` or `false`, which either enables or disables its
   execution during the benchmark experiment. The `repetition` parameter
   changes how often the query is repeated with varying parameters. See
   [Query configuration](../../../configuration/README.md) for the full
   format. After adjusting the files, run the python script
   `generateParameterVariations.py` found in the `scripts` folder of each
   scenario (from that `scripts` folder). This creates the queries with
   parameters, found afterward in the `queries` folder, split into separate
   files per database system. These files are also automatically copied into
   `client/benchmark_client/src/main/resources/<scenario>/`, which is what the
   client actually reads at runtime.
2. **Build the client.** Maven is required.
   ```sh
   cd client/benchmark_client
   mvn clean install
   ```
3. **Point the client at your cluster.** By default the client targets a host
   named `geobenchr-sut-5` with the credentials of this project's own
   single-box test setup. To target a different cluster (e.g. a partner-hosted
   one), set environment variables instead of editing source — see
   [Environment variables](#environment-variables) below. SpaceTime has one
   extra prerequisite beyond an env var; see
   [Running against SpaceTime](#running-against-spacetime).
4. **Choose which systems run**, if you don't want all of them — see
   [Choosing which systems run](#choosing-which-systems-run).
5. **Run a benchmark**, one scenario at a time (from `client/benchmark_client`):
   ```sh
   export MAVEN_OPTS="-Dfile.encoding=UTF-8"
   mvn -q compile exec:java -Dexec.args=<ais|aviation|cycling|geolife>
   Example: SYSTEMS=ClickHouse,DuckDB mvn -q compile exec:java -Dexec.args=aviation
   ```
   For the full benchmark, with each system running alone on the cluster and
   CPU/memory sampled on every node, see
   [Isolated benchmark runs](#isolated-benchmark-runs).
6. **Check [Output](#output)** for where results land.

## Environment variables

All connection details are read from environment variables at startup (both
`BenchmarkClient.java` and the executors read them). Unset variables fall back to this project's
own single-box test defaults, so pointing at a different cluster (e.g. a
partner-hosted one) needs no source edits.

| Variable | Default | Used by | Meaning |
| --- | --- | --- | --- |
| `SUT_HOST` | `geobenchr-sut-5` | PostGIS, MobilityDB, DuckDB, ClickHouse, Sedona, SpaceTime | Hostname/IP of the system(s) under test. |
| `NUM_THREADS` | `1` | all | Number of concurrent query threads within one client process. |
| `SUT_DB_PASSWORD` | `test` | PostGIS, MobilityDB | Password for the `postgres` user on the SUT's Postgres/Citus instance. |
| `SPARK_MASTER` | `yarn` | Sedona | Spark master URL Sedona submits jobs to; override for e.g. `local[*]` off-cluster testing. |
| `SPACETIME_PORT` | `31340` | SpaceTime | Port of the SpaceTime (MSQL) coordinator. |
| `SPACETIME_PYTHON` | `~/bench/.venv/bin/python` | SpaceTime | Python interpreter with `pymsql` installed. |
| `SPACETIME_RUNNER_SCRIPT` | `~/bench/spacetime_query_runner.py` | SpaceTime | Path to the bridge script (see below). |
| `SYSTEMS` | all six | all | Comma-separated subset to run, e.g. `PostGIS,ClickHouse`. |
| `SPARK_CONF` | — | Sedona | Extra Spark settings, `key=value;key=value` (executors, `spark.sql.session.timeZone=UTC`, ...). |
| `SEDONA_GEOPARQUET` | — | Sedona | Point tables to read as GeoParquet (`hdfs:///geobenchr/<table>.geoparquet`); all four point tables in the current setup. |
| `RESULTS_OUT` | — | Sedona | Append every result as one JSON line to this file (equivalence checks). |
| `DUCKDB_CANCEL_CMD` | — | DuckDB | Shell command that restarts the DuckDB server; run when a DuckDB query times out (see below). |

Example, targeting a different cluster with 4 threads:
```sh
SUT_HOST=nst-1 NUM_THREADS=4 SUT_DB_PASSWORD=postgres mvn -q compile exec:java -Dexec.args=aviation
```

## Choosing which systems run

Set `SYSTEMS` to a comma-separated list (`SpaceTime`, `PostGIS`, `MobilityDB`,
`DuckDB`, `ClickHouse`, `Sedona`); unset, all six run. A system without a query
for some scenario (e.g. MobilityDB has no Geolife queries) skips it.

## Running against SpaceTime

SpaceTime (MSQL) is a partner-hosted engine with no JDBC driver — its only
client library, `pymsql`, is Python-only and not on public PyPI (see
[SpaceTime cluster access](spacetime-cluster-access.md) for how to obtain it
and connect to the cluster). To let the Java client still drive it,
`SpaceTimeExecutor` shells out to
[`scripts/spacetime_query_runner.py`](../scripts/spacetime_query_runner.py),
which runs SQL over `pymsql`. During a benchmark it runs as a long-lived worker
(`--serve`, one per benchmark thread, one open `pymsql` connection each); run
without `--serve` it reads one statement from stdin and reports `ROWS:<n>`.

Before running SpaceTime queries, deploy that bridge on whichever machine
actually runs `benchmark_client` — it needs network access to the SpaceTime nodes (e.g.
`nst-inserter`, the only box with a route to the cluster's private network,
per [spacetime-cluster-access.md](spacetime-cluster-access.md)):

```sh
cp -r <bench directory provided by the SpaceTime team> ~/bench   # brings a venv with pymsql already installed
cp scripts/spacetime_query_runner.py ~/bench/
```

This matches `SPACETIME_PYTHON`/`SPACETIME_RUNNER_SCRIPT`'s defaults
(`~/bench/.venv/bin/python`, `~/bench/spacetime_query_runner.py`), so no env
vars are needed if you deploy to that exact path; override them if you deploy
elsewhere. SpaceTime itself needs no authentication — just a reachable
`SUT_HOST`/`SPACETIME_PORT` (default `31340`).

Sanity-check the bridge directly before trusting a full benchmark run through it:
```sh
echo 'SELECT COUNT(*) FROM rides' | ~/bench/.venv/bin/python ~/bench/spacetime_query_runner.py \
    --host <SUT_HOST> --port 31340
```
See [Sample queries per SUT](sample-queries.md) for more verified smoke-test
queries per system, and [spacetime-cluster-access.md](spacetime-cluster-access.md)
for cluster topology, data layout, and known caveats (e.g. the maritime
tables need an explicit time filter).

## What a timing measures

Since 2026-10-07 every executor reuses its sessions: each benchmark thread
borrows an open session (a JDBC connection, or a SpaceTime worker process),
runs the query, and hands the session back. Opening a session, and one untimed
warm-up `SELECT 1` on it, happens outside the measurement. A timing therefore
covers sending the query, executing it, and reading every result row on the
client. Earlier runs opened a new connection (and, for SpaceTime, a new Python
process) per query inside the timing, which added ~6 ms (ClickHouse) to ~105 ms
(SpaceTime) per query, so their timings aren't comparable with newer ones.

Each query may run 600 s, counted from its own start. A query past that is
logged as `TIMEOUT` and stopped on the server before the next one starts
(since 2026-10-09; before, it kept running and the queries queued behind it
were wrongly logged as timeouts): `Statement.cancel()` for PostGIS, MobilityDB
and ClickHouse (plus a server-side limit of 630 s as a backstop), killing the
worker process for SpaceTime, cancelling the Spark jobs for Sedona. The DuckDB
driver's cancel does nothing on the server and DuckDB has no statement
timeout, so a DuckDB timeout runs `DUCKDB_CANCEL_CMD`, which restarts the
DuckDB server. The DuckDB driver's HTTP request timeout is 660 s (default
60 s). Queries a system has no translation for (empty SQL in its YAML) are
skipped, not timed.

## Write queries and resetting the data

The benchmark's write queries are insert/update only (delete queries are disabled in every
scenario's `combinedBenchConf.yaml`). They only touch negative test ids, so after any run that
included them, restore the pure dataset by running
[`scripts/reset_benchmark_writes.sh`](../scripts/reset_benchmark_writes.sh) on `nst-1`. It deletes
every negative-id row from the four point tables in Postgres (PostGIS/MobilityDB), ClickHouse and
DuckDB, then prints the remaining row counts per system.

## Output

Each system's queries log to stdout/stderr as they run
(`[<thread>] [<dialect>] [<application>] SUCCESS: '<query>': <ms>ms`, or
`TIMEOUT`/`ERROR`), and successes/timeouts/errors are also appended to a
per-system log file in the current directory:
```
<dialect>query_results_<runID>.log
Example: PostGISquery_results_4084d.log
```
`runID` is a random 5-character ID generated once per client process. A
separate `benchmark_usage_<runID>.log` marker file records start/end markers
for each system's phase within the run. Every log line carries
`[rep=<n> sql=<hash> at=<time>]`, which
[`benchmark/results/build_results.py`](../../../results/build_results.py) uses
to match executions with their parameters.

## SedonaDB vs. Sedona

These are two different systems, easy to confuse:

- **SedonaDB** is driven separately, via notebook — see the Jupyter Notebook
  at [`benchmark/sedonadb.ipynb`](../../../sedonadb.ipynb), which contains all
  steps including benchmark execution and data loading. `benchmark_client`
  does not talk to SedonaDB at all.
- **Sedona** (Apache Sedona's SQL extensions over Spark/YARN) *is* one of the
  six systems `benchmark_client` drives directly (`SedonaExecutor`, submitting
  `spaceTimeSQL`-analogous SQL as Spark jobs via `spark.master=yarn` by
  default — override with `SPARK_MASTER`). It's covered by everything above
  like any other dialect.

## Isolated benchmark runs

[`configuration/scripts/realbench/`](../../../configuration/scripts/realbench/) holds the run setup:

| Script | Where | Purpose |
|---|---|---|
| `build_jar.sh OUT.jar [REPS]` | any machine with Maven | generates the query files for a run (every read query `REPS` times, 52 by default, each with its own parameters; write queries off) on a copy of `benchmark/` and builds the client jar with them |
| `make_thread_jars.sh OUT_DIR [threads...]` | any machine with Maven | one jar per client-thread count (default 1, 4, 8): per query the same 128 measured parameter sets plus one warm-up repetition per thread |
| `pg_tuning.sh`, `mem_tuning.sh` | nst-1 | apply the memory and parallelism settings described in the [main README](../../../../README.md#configuration) to PostgreSQL, ClickHouse and YARN on all nodes |
| `svc.sh only <System>` / `svc.sh all` | nst-1 | stops every benchmarked system except the one under test on all five nodes, restarts that one so it starts on otherwise idle nodes, and waits until it is healthy; `all` starts everything again |
| `real_bench_threads.sh [threads...]` | nst-inserter | runs `real_bench_v3.sh` with 1, 4 and 8 client threads, one after the other |
| `THREADS=n real_bench_v3.sh` | nst-inserter | runs each system that way through all four scenarios with n client threads, samples CPU/memory/disk/network on every node ([`hw_sampler.py`](../../../configuration/scripts/hw_sampler.py)), starts everything again at the end and builds the result tables ([`benchmark/results/`](../../../results/)) |

Sedona's point tables are built with
[`sedona_geoparquet/build_geoparquet_layout.sh`](../../../configuration/scripts/sedona_geoparquet/build_geoparquet_layout.sh)
and read with `SEDONA_GEOPARQUET=crossing_points,flight_points,ride_points,geolife_points`.

With several client threads (`NUM_THREADS`), a query that runs past the timeout is stopped on its own
(`Statement.cancel` for PostgreSQL and ClickHouse, its own worker process for SpaceTime, its own Spark job
group for Sedona) while the other threads' queries continue. DuckDB can only be stopped by restarting its
server, which ends the other threads' running queries too; those are logged as `INTERRUPTED`. The other scripts
in `realbench/` are the setups of earlier runs.

The earlier single-node paper setup (`run_benchmark.sh`, which restarted the
local Postgres and recorded `sar` samples), the distributed manager/worker mode
and other code no longer used are kept locally in `single-node/`, outside git.
