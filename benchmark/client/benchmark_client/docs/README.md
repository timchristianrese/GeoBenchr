# benchmark_client documentation

Central documentation for `benchmark_client`, the Java benchmark client. This
assumes the `server` and `data` installation steps (see the root
[README.md](../../../../README.md)) are already done.

- [Installation and running](#installation) — build the client, configure which
  systems to test, run a single-client benchmark.
- [SedonaDB](#sedonadb) — SedonaDB is driven separately, via notebook.
- [Distributed execution (manager/worker)](#distributed-execution-managerworker) —
  spreading query execution across multiple client processes/machines.
- [Query configuration](../../../configuration/README.md) — how the query files
  the client reads (`combinedBenchConf.yaml`, `generateParameterVariations.py`,
  etc.) are defined and generated; lives in `benchmark/configuration/` since it
  isn't specific to the Java client.
- [SpaceTime cluster access](spacetime-cluster-access.md) — internal, gitignored
  notes for connecting to a partner-hosted SpaceTime cluster (credentials/IPs;
  not for publication).

## Installation

The benchmark client is split into the `client` itself and necessary
`configuration` files. This installation assumes that you have already set up
the database systems which you would like to benchmark.

Please notice that the benchmark client for SedonaDB is managed separately, as
currently the interaction patterns for that platform are limited to Python and
R — see [SedonaDB](#sedonadb) below. For all other platforms, the client is
centralized.

To run a benchmark, the following steps are required (assuming completion of
the `server` and `data` installation processes):

- Check the configuration files found in scenario folders under
  `benchmark/configuration` to adjust the queries, repetitions, and which
  queries you want executed. Each query in `combinedBenchConf` can be toggled
  to `true` or `false`, which either enables or disables its execution during
  the benchmark experiment. The `repetition` parameter changes how often the
  query is repeated with varying parameters. See
  [Query configuration](../../../configuration/README.md) for the full format.
- After adjusting the files, run the python script
  `generateParameterVariations` found in the `scripts` folder of each scenario
  (from the `scripts` folder). This actually creates the queries with
  parameters, which can then be found in the `queries` folder, split into
  separate files based on the database system. These files are also
  automatically transferred into the `client` folder, as they are used by it
  to run the benchmark.
- Adjust the client to reflect which database systems you want to evaluate,
  and the IP/hostname of your database system (Line 27 and 38). In
  `benchmark/client/benchmark_client/src/main/java/benchmark_client/BenchmarkClient.java`,
  comment or uncomment the code blocks which you want to run experiments for
  so that the client only runs the desired ones. For example, assuming you do
  not want to run TimescaleDB experiments, comment the code section 81-84.
  Save your changes afterwards.
- Now we build the benchmark client. For this, Maven is required. From this
  folder (`benchmark`), run the following commands to build the benchmark
  client:
  ```
  cd client/benchmark_client
  mvn clean install
  ```
- Afterwards, you can choose which benchmark you want to run simply by
  running:
  ```
  ./run_benchmark.sh <ais|aviation|cycling>
  Example: ./run_benchmark.sh ais
  ```
- You can also run all benchmarks twice. Results can be found in this folder,
  with the following identifiers:
  ```
  <platform>query_results_<random_id>.log
  Example: PostGISquery_results_4084d.log
  ```

## SedonaDB

The Jupyter Notebook at [`benchmark/sedonadb.ipynb`](../../../sedonadb.ipynb)
contains all steps for SedonaDB, including benchmark execution and data
loading.

## Distributed execution (manager/worker)

The steps above run everything from a single machine/process. To spread query
execution across multiple workers — other machines, or just separate client
processes on one machine — use the manager/worker mode instead of
`run_benchmark.sh`. It reuses the same generated query files and the same
`QueryExecutor`/dialect code as the single-client mode, so no extra setup
beyond a normal `mvn clean install` in `client/benchmark_client` is needed.

1. On one machine, start the manager for the scenario you want to run:
   ```
   ./run_manager.sh <application> [dialects] [port]
   Example: ./run_manager.sh aviation
   Example: ./run_manager.sh aviation DuckDB,ClickHouse 8787
   ```
   `dialects` defaults to the same set the single-client mode runs (SpaceTime,
   PostGIS, MobilityDB, DuckDB, ClickHouse). The manager builds the full list
   of query executions up front and hands them out to whichever workers ask
   for one — it prints its port and generated run ID, and a live progress
   summary while workers are connected. It also has `Connection: keep-alive`-
   free HTTP endpoints you can `curl` directly: `GET /runid`, `GET /task`,
   `POST /complete`, `GET /status`.
2. On each worker machine (or additional terminal for a second local client),
   point a worker at the manager:
   ```
   ./run_worker.sh <managerHost:port> [numThreads] [dialects]
   Example: ./run_worker.sh geobenchr-client-1:8787
   Example: ./run_worker.sh geobenchr-client-1:8787 4 DuckDB,ClickHouse
   ```
   `numThreads` defaults to the `NUM_THREADS` env var (same convention as the
   single-client mode), and an optional `dialects` filter restricts which
   dialects that particular worker will pull — useful since
   SpaceTime/GeoMesa/Sedona need local CLI tools or Spark that not every
   machine will have installed. Workers pull tasks one at a time as they
   finish the last one, so load balances naturally across however many you
   start, even with uneven query costs.
3. Each worker writes its own local result/marker logs exactly like the
   single-client mode does (`<dialect>query_results_<runID>.log`,
   `benchmark_usage_<runID>.log`), all stamped with the *same* run ID fetched
   from the manager, so logs from different machines can be correlated
   afterward. The manager itself only tracks lightweight progress counts, not
   full query results. By design there's no task retry if a worker dies
   mid-task (that task is simply lost), and the manager's HTTP endpoints have
   no authentication — both fine for a trusted internal network, but keep
   them off anything public. If multiple worker *processes* share one
   machine/directory, their per-query result logs interleave safely, but only
   the *last* one to finish will keep its `benchmark_usage_<runID>.log`
   marker file (each `moveLogFile()` overwrites the same target).

The manager exits automatically once every dispensed task has been reported
complete by a worker.
