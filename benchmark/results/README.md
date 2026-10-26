# GeoBenchr results

Six systems — PostGIS, MobilityDB, ClickHouse, DuckDB, SpaceTime and Apache Sedona — answer the same 48 read
queries over the same data on a five-node cluster. Each system runs alone on the cluster; every query runs with
128 measured parameter sets. The configuration of every system is described in the
[main README](../../README.md#configuration).

## Runs

### Experiment of 2026-10-22: parameters where the data is, three workloads

Parameters are drawn where each scenario's data is, so no query looks for data that isn't there, and every measured
parameter set of a query is distinct (128 per query; `countFlightsInCounties` has 53 counties and
`countRideInDistrict` 12 districts). Each workload runs with 2, 4, 8 and then 1 client threads; within a workload,
every run measures the same parameter sets, and the read-only and mixed workloads share the same reads.

| Workload | Client threads | Run | Status | Results |
|---|---:|---|---|---|
| read-only | 2 | 2026-10-22 to 2026-10-23 | done | [`2026-10-22/read-only/2-thread/`](2026-10-22/read-only/2-thread/README.md) |
| read-only | 4 | 2026-10-23 to 2026-10-24 | done | [`2026-10-22/read-only/4-thread/`](2026-10-22/read-only/4-thread/README.md) |
| read-only | 8 | 2026-10-24 | done | [`2026-10-22/read-only/8-thread/`](2026-10-22/read-only/8-thread/README.md) |
| read-only | 1 | 2026-10-24 to 2026-10-26 | done | [`2026-10-22/read-only/1-thread/`](2026-10-22/read-only/1-thread/README.md) |
| 75 % reads / 25 % writes | 2, 4, 8, 1 | | pending | PostGIS, MobilityDB, ClickHouse, DuckDB |
| 90 % reads / 10 % writes | 2, 4, 8, 1 | | pending | PostGIS, MobilityDB, ClickHouse, DuckDB |

In the mixed workloads, writes (inserts, updates, bulk inserts of 10 rows) make up a quarter or a tenth of the
measured executions; after each system, everything they added is removed again (`purge_writes.sh`). Sedona has no
write support (GeoParquet files on HDFS) and runs read-only only; SpaceTime's write support is pending.

### Experiment of 2026-10-14: read-only, parameters uniform over each scenario's extent

| Run | Client threads | Status | Results |
|---|---:|---|---|
| 2026-10-14 to 2026-10-16 | 1 | done | [`read-only/1-thread/`](read-only/1-thread/README.md) |
| 2026-10-16 | 4 | done | [`read-only/4-thread/`](read-only/4-thread/README.md) |
| 2026-10-16 to 2026-10-17 | 8 | done | [`read-only/8-thread/`](read-only/8-thread/README.md) |

All three runs use the same queries, the same 128 parameter sets per query and the same configuration; they differ
only in the number of client threads taking queries from one shared queue (and in the warm-up: one repetition per
client thread).

The read-write workloads are part of the experiment of 2026-10-22 (above).

### Superseded

Earlier runs with an earlier configuration (data layouts, indexes, memory settings), kept for reference only —
please do not cite:

| Run | Repetitions | Results |
|---|---:|---|
| 2026-10-09 to 2026-10-11 | 52 | [`superseded/run_52rep_2026-10-09/`](superseded/run_52rep_2026-10-09/README.md) |
| 2026-10-08 | 5 | [`superseded/run_5rep_2026-10-08/`](superseded/run_5rep_2026-10-08/README.md) |

## How a run measures

- **Queries and parameters.** 48 read queries, 12 per scenario (AIS, aviation, cycling, Geolife): six that are the
  same in every scenario (`SpatialBBox`, `kNN`, `timeInterval`, `timeAggregation`, `spatioTemporalRange`,
  `IDsInsideRegion1Andsecond_region`) and six specific to each. Each system runs its own translation; all get the
  same parameters, drawn by a seeded generator within each scenario's data extent (area and time span). MobilityDB
  has no Geolife queries (check-in data has no trajectories), ClickHouse no `CountRidesFullyInDistrictRadius`. The
  exact queries of a run are in its `raw/queries/`.
- **One system at a time.** Before each system's run, every other benchmarked system is stopped on all five nodes
  and the system under test is restarted and checked. Order: ClickHouse, SpaceTime, DuckDB, PostGIS, MobilityDB,
  Sedona; within a system AIS, aviation, cycling, Geolife.
- **Client.** One benchmark client on a separate host; with *n* client threads, *n* queries run at the same time.
  Each thread keeps one open session for the whole run; connecting is not timed.
- **Warm-up.** The first repetition per client thread of every query is warm-up and not counted.
- **A timing** covers sending the query, executing it and reading every result row on the client.
- **Timeout:** 600 s per query. A query past that is stopped on the server before its thread takes the next one.
  A parameter set on which every system times out is left out for every system.
- **Hardware:** CPU, memory, disk and network of the five nodes and the client host, every 5 s.

## Ranking

A system is ranked on a query only if all its measured repetitions succeeded and its results match PostGIS. Results
were compared on five parameter sets of every query before the runs: identical; numbers within 1 %; for
nearest-neighbour queries, the same rows up to the order of ties at equal distance; an empty answer either as no row
or as one row of zeros. The fastest ranked system by mean runtime wins a query; a system's *slowdown* on a query is
its mean divided by the winner's.

## Scripts

| Script | What it does |
|---|---|
| [`build_results.py`](build_results.py) | raw client logs, query files and hardware samples of a run → CSV tables, `tables.md` and plots (`RESULTS_DIR=<run folder> WARMUP_REPS=<client threads> python3 build_results.py`) |
| [`empty_results.py`](empty_results.py) | how often each query returned no row (single-thread runs) |
| [`make_run_readme.py`](make_run_readme.py) | the README of a run folder from its CSV tables (`python3 make_run_readme.py read-only/1-thread 1`) |
| [`make_notebook.py`](make_notebook.py) | an analysis notebook for a run |

ClickHouse is the only system for which we define functions (H3 cell covers); see
[`clickhouse_additions.md`](clickhouse_additions.md).
