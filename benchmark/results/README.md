# GeoBenchr results

Six systems — PostGIS, MobilityDB, ClickHouse, DuckDB, SpaceTime and Apache Sedona — answer the same 48 read
queries over the same data on a five-node cluster. Each system runs alone on the cluster; every query runs with
128 measured parameter sets. The configuration of every system is described in the
[main README](../../README.md#configuration).

## Runs

### Read-only

| Run | Client threads | Status | Results |
|---|---:|---|---|
| 2026-10-14 to 2026-10-16 | 1 | done | [`read-only/1-thread/`](read-only/1-thread/README.md) |
| 2026-10-16 | 4 | running | `read-only/4-thread/` (added when the run has finished) |
| after the 4-thread run | 8 | pending | `read-only/8-thread/` (added when the run has finished) |

All three runs use the same queries, the same 128 parameter sets per query and the same configuration; they differ
only in the number of client threads taking queries from one shared queue (and in the warm-up: one repetition per
client thread).

### Read-write

[`read-write/`](read-write/README.md): not measured yet; the write queries are switched off, so every run reads
the same, unchanged data.

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
