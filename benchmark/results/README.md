# GeoBenchr results — 5-repetition run (2026-10-07/08)

> **Status: run in progress.** This page is filled in from `results/` once the run has finished
> (`python3 build_results.py`, then the tables below are refreshed from `results/tables.md`).

## What was measured

Six systems answer the same 48 queries (12 per scenario: AIS maritime, aviation, Berlin cycling,
Geolife) over the same data:

| System | Where it runs | Query language |
|---|---|---|
| PostGIS | Citus 5-node cluster (nst-1 coordinator, nst-2..5 workers) | SQL + PostGIS |
| MobilityDB | same Citus cluster, trajectory tables (`tgeompoint`, discrete), range-distributed tiles | SQL + MobilityDB |
| ClickHouse | 5-node cluster (`cluster_1`), point tables sorted by `(h3, timestamp, id)` | ClickHouse SQL |
| DuckDB | single node (nst-1), quack server | DuckDB SQL + spatial |
| SpaceTime | partner engine on nst-1..5 | SpaceTime SQL |
| Sedona | Spark on YARN over HDFS parquet (nst-1..5) | Spark SQL + Sedona |

* **Repetitions:** every query runs 5 times per system, **each time with different parameters**
  (time periods, regions, radii, harbours, ...). The parameters come from a seeded generator, so all
  systems get exactly the same 5 parameter sets per query.
* **Read queries only.** Write queries are disabled; the data is not modified.
* **One system at a time, one query at a time** (`NUM_THREADS=1`), scenarios in the order AIS,
  aviation, cycling, geolife. SpaceTime, PostGIS, MobilityDB, DuckDB and ClickHouse run from
  nst-inserter (2026-10-07 22:52 – 2026-10-08 03:00); Sedona runs separately from nst-1 (its Spark
  driver fails from nst-inserter), see *Sedona* below.
* **What one timing covers:** sending the query, executing it, and reading every result row on the
  client. Each system keeps one open session per benchmark thread; opening it and one warm-up
  `SELECT 1` happen outside the measurement (since 2026-10-07; earlier runs paid a new connection per
  query, ~6 ms for ClickHouse up to ~105 ms for SpaceTime, inside the timing).
* **Timeout:** 600 s per query for every system (DuckDB's driver timeout raised from 60 s to 660 s).
  No query timed out.
* **Result transfer:** every system's client reads all result rows. PostgreSQL results are streamed
  in batches of 10,000 rows (a multi-million-row result had run the 1 GB client out of memory).
* **No query for a system** (e.g. MobilityDB has no trajectory form of most Geolife queries) =
  "n/a"; such queries are skipped, not timed.

### Equivalence: do the systems answer the same question?

Before the run, every system's queries were checked against PostGIS on the first parameter set:
actual values for results up to 2,000 rows, full row counts above that. A system only counts as the
fastest on a query if all 5 repetitions succeeded and its result matched PostGIS:

* **identical**, or **close** (every number within 1 %, e.g. a few duplicate or border points), or
* **kNN** (same number of rows; differs only in geometry output format and ties at equal distance).

Anything else is a **major difference** (marked ≠) and is not ranked. The ClickHouse queries using the
H3 index were additionally compared with the previous scan-only ClickHouse queries on **all 5**
parameter sets (row count + hash of the full result).

### ClickHouse uses its H3 primary index

ClickHouse's point tables are sorted by H3 cell (resolution 9). Spatial queries now start with a
filter on that cell (`h3 GLOBAL IN (<cells covering the search area>)`), which lets ClickHouse skip
everything outside the area — its native spatial index, the counterpart of PostGIS's GiST index. The
cell cover is a superset built by SQL functions in `configuration/scripts/clickhouse_functions.sql`
(coarse res-7 cells of the shape and its densified outline, widened for the search distance,
expanded to res-9 children); the exact spatial predicate is unchanged and decides the result. The
previous scan-only versions are kept in the configs as `clickhouse_scan` (not run). Projected
EPSG:3857 coordinates are stored as `x`/`y` columns, like PostGIS stores 3857 geometries.

### Sedona

* Runs from nst-1 with 36 static Spark executors (1 core, 2 GB each; YARN allows at most one core per
  container) on nst-2, nst-4 and nst-5. nst-1 is excluded (memory-tight: DuckDB server and Postgres
  coordinator), nst-3's YARN NodeManager is marked unhealthy (disk 95 % full). The cluster's default
  Spark settings (dynamic allocation, at most 4 single-core executors) left the cluster ~95 % idle and
  were not used for the results.
* Spark session time zone UTC: the Parquet files on HDFS store the same wall-clock values as Postgres as
  UTC instants; with the driver's default Europe/Berlin zone every time literal was read 1–2 h off.
* `ST_Transform` with explicit `'EPSG:4326', 'EPSG:3857'` (Sedona rejects the bare SRID PostGIS takes),
  geometry columns collected as WKB, query execution (not just planning) inside the timing.

### Corrections after the main run

Each was found by checking the results, fixed, verified, and the affected executions re-run; the
re-runs replace the original executions in all tables (`results/raw/logs/rerun_*`, originals kept):

| What | Affected | Re-run |
|---|---|---|
| ClickHouse closed coastlines into polygons (one crossing too many on coastline 45); now point-to-segment distance | `countActiveCrossingsNearIsland`, `islandOrCoastLineCrossingsInPeriod` (ClickHouse) | 10 executions, identical to PostGIS on all 5 parameter sets |
| Client out of memory on a multi-million-row result (one repetition missing for PostGIS and MobilityDB) | `CountRidesFullyInDistrictRadius` | 10 executions with streamed results |
| Sedona: CRS syntax, time zone, geometry decoding, executor resources | all Sedona queries | complete Sedona run (240 executions) |

### Hardware monitoring

`configuration/scripts/hw_sampler.py` ran on nst-1..5 throughout (5 s samples): CPU (user/system/
iowait/idle), memory, swap, disk and network throughput per node, plus CPU and resident memory per
process group (postgres, clickhouse, duckdb, spacetime, java = Spark/YARN/HDFS). `hardware_by_phase.csv`
summarizes it per (scenario, system) phase; `plots/hardware_timeline.png` shows the whole run.

## Files

| Path | Content |
|---|---|
| `results/executions.csv` | every query execution: scenario, system, query, repetition, status, duration, parameters, SQL |
| `results/summary.csv` | per (scenario, query, system): runs, failures, mean/median/min/max/std, equivalence, rank, slowdown |
| `results/winners.csv` | per query: fastest system, runner-up, margin, full ranking, systems not ranked and why |
| `results/system_scores.csv` | per system: wins, completed queries, geometric-mean slowdown vs the fastest |
| `results/equivalence.csv` | per (scenario, query, system): result compared with PostGIS |
| `results/h3_vs_scan.csv` | ClickHouse H3 vs scan-only queries, every repetition |
| `results/hardware_by_phase.csv` | CPU/memory/disk per node and CPU per process group during each system's run |
| `results/tables.md` | the tables below, generated |
| `results/plots/` | one plot per query (`queries/`), one overview per scenario, wins, slowdown heatmap, hardware timeline |
| `results/raw/` | the inputs: client logs, the exact query files run, equivalence results, hardware samples |
| `build_results.py` | turns `results/raw/` into everything above |
| `analysis.ipynb` | notebook over the CSVs (generated by `make_notebook.py`) |

## Results

_Filled in after the run._
