# Database State (snapshot: 2026-09-22, nst-cluster)

Cluster: `nst-1`..`nst-5` (5 nodes, 62GB RAM / node), reached via jump host `tim@2.28.3.43`
(`nst-inserter`) → `tim@10.1.0.11` (`nst-1`, private VLAN `10.1.0.11`-`.15`). All 6 engines run on
`nst-1`..`nst-5`; the benchmark client itself is meant to run from `nst-inserter` (see
[spacetime-cluster-access.md](../benchmark/client/benchmark_client/docs/spacetime-cluster-access.md),
gitignored) so client overhead doesn't compete with the systems under test for CPU/RAM.

## Engines and versions

| Engine | Version | Port (private IP) | Notes |
|---|---|---|---|
| PostgreSQL / PostGIS / MobilityDB / Citus | PostgreSQL 17.11, PostGIS 3.6.4, MobilityDB 1.1.2, Citus 14.1 | 5433 | Citus-distributed; connect as OS/db user `postgres` (peer auth — use `sudo -u postgres psql`, not `psql -U postgres` over a socket as another OS user), db name `postgres` |
| ClickHouse | 26.8.2.7 | 9500 (native), 8123 (HTTP) | 5-shard `cluster_1`, `ReplicatedMergeTree` + `Distributed` wrapper per table |
| DuckDB | v1.5.5 | 9494 (Quack RPC server, `quack-jdbc` driver) | Single file `/var/lib/duckdb/geobenchr.duckdb` on `nst-1` only — not distributed. The Quack systemd service (`duckdb-quack`) holds an exclusive file lock; stop it before any direct-file CLI access, restart after |
| Apache Sedona / Spark | Sedona 1.8.1, Spark 4.1.2 (Hadoop 3.4.2), Scala 2.13 | YARN (RM/HDFS on `nst-1`) | Reads pre-exported Parquet from HDFS (`hdfs:///geobenchr/<table>.parquet`), not live Postgres — see "Sedona export" below |
| SpaceTime | (version unavailable via CLI without extra perms) | 31340 | `kdtree` indexes on every table; data + engine both live on `nst-1`..`nst-5` |

## Point tables (row counts as of 2026-09-22)

| Table | Postgres | ClickHouse | DuckDB | Sedona/HDFS | SpaceTime |
|---|---|---|---|---|---|
| `flight_points` (aviation) | 2,703,631,682 | 2,703,631,682 | 2,703,631,682 | 2,703,631,682 (124 partitions, ~450MB each) | not independently re-verified this session (config declares 500,000,000 as a capacity/planner stat, not a live count) |
| `ride_points` (cycling) | 1,178,625,381 | 1,178,625,381 | 1,178,625,381 | exported, 256 partitions | not independently re-verified (config: 1,000,000,000, likely a stat) |
| `geolife_points` | 3,960,126,695 | 3,960,126,695 | 3,960,126,695 | exported, 128 partitions | not independently re-verified |
| `crossing_points` (ais) | 3,817,218,804 | 3,817,292,082 (0.0019% gap, accepted — see COORDINATION.md) | 3,817,218,804 | exported, 256 partitions | not independently re-verified (config: 10,000,000,000, likely a stat) |

All 4 point tables carry `id bigint`/`UInt64` (crossing_points was migrated from a broken `varchar`
id this session), a `geom` column (`geometry(Point,3857)` in Postgres/DuckDB, WKB bytes in the
Sedona Parquet export, `lat`/`lon` scalars + materialized `Point`/`h3` in ClickHouse), and a
`timestamp`/`t` column. `flight_points`/`ride_points`/`crossing_points` additionally carry a
`line_geom` (connects consecutive points of the same trip; NULL across month-partition boundaries
for `crossing_points`, see COORDINATION.md) — `geolife_points` has no `line_geom` at all.

## Trip-summary tables (one row per trip)

Distinct from the MobilityDB-native `flights`/`rides`/`crossings` tgeompoint tables below — these
are generic, engine-portable tables: `id, start_time, end_time, num_points, start_location,
end_location` (+`min_altitude`/`max_altitude` for flights, +`rider_id` for rides). Rebuilt
2026-09-22 from current point-table data (previous copies were stale — see COORDINATION.md for the
rebuild methodology, including a ClickHouse Distributed-table `INSERT...SELECT` fan-out bug hit and
fixed along the way).

| Table | Postgres | ClickHouse | DuckDB | Sedona/HDFS |
|---|---|---|---|---|
| `flights_summary` | 6,760,000 | 6,760,000 | 6,760,000 | 6,760,000 |
| `rides_summary` | 50,000,020 | 50,000,020 | 50,000,020 | 50,000,020 |
| `crossings_summary` | 2,018,208 | 2,018,227 (19-row / 0.0009% gap) | 2,018,208 | 2,018,208 |

**Query-YAML naming was out of sync with the live schema** (`combinedBenchConf.yaml` referenced
`ride_trajectories`/`crossing_trajectories`/`origin_geom`/`destination_geom`, and Sedona's queries
expected a split `<x>_start`/`<x>_end` table pair) — fixed 2026-09-22 in
`benchmark/configuration/{cycling,ais}/config/combinedBenchConf.yaml` to reference the real
`rides_summary`/`crossings_summary` tables and `start_location`/`end_location` columns, and to
collapse Sedona's stale split-table JOINs into single-table queries. All 5 affected queries
(`averageRideDurationToGoAtUniversityInPeriod`, `averageRideDurationDuringDay`,
`countActiveCrossingsInPeriod`, `averageDurationCrossingStartAtHarbourInPeriod`,
`crossingsConnectTwoGivenHarbours`) are currently `use: false` in the source config, so this was
dormant breakage, not an active failure — see docs/query-catalog.md. **Not yet regenerated** into
`src/main/resources/*/` or rebuilt into the deployed jar. **Aviation has no equivalent
non-MobilityDB query at all** (not a naming bug — no query referencing `flights_summary` exists in
any dialect block for aviation) — a content gap, not touched here.

## MobilityDB-native trajectory tables (`tgeompoint`, Postgres/Citus only)

| Table | Row count | Index |
|---|---|---|
| `flights` | 6,760,000 | `flights_trip_gist_idx` GiST(`trip`) |
| `rides` | not re-verified this session | `rides_trip_gist_idx` GiST(`trip`) |
| `crossings` | 2,018,208 | `crossings_trip_gist_idx` GiST(`trip`) |

## Indexes

**Postgres**: every point table has a unique btree on `(id, timestamp)`, a plain btree on `id` and
on `timestamp`, and a GiST on `geom` (+ GiST on `line_geom` where present); `crossing_points` is
partitioned by `timestamp` range (19 monthly partitions). All 3 `_summary` tables have a btree PK
on `id`, GiST on `start_location`/`end_location`, and a btree on `start_time`.

**ClickHouse**: no secondary-index concept — the `ORDER BY` clause on each `ReplicatedMergeTree`
*is* the (sparse primary) index. All 4 point tables and all 3 summary tables use
`ORDER BY (start_h3 or h3, start_time or timestamp, id)`, where `h3`/`start_h3` is a `MATERIALIZED
geoToH3(lat, lon, 9)` column — an H3-cell-then-time-then-id clustering, not a true spatial index.

**DuckDB**: RTREE spatial indexing on the 4 billion-row point tables has failed 5 separate times
(2 tables, 13.9GB–55GB memory limits) with a hard OOM — confirmed a real architectural ceiling for
tables this large, **not attempted further, on hold per user decision** (see COORDINATION.md).
The 3 `_summary` tables are 1–3 orders of magnitude smaller (2M–50M rows) and their RTREE indexes
on `start_location`/`end_location` **built successfully in under 16 seconds total** — direct
confirmation the OOM is about raw table size, not RTREE itself.

**Sedona**: no persistent index — each query reads the full HDFS Parquet export (Parquet
row-group min/max stats give some pruning where the export was sorted, e.g. by `timestamp`).

**SpaceTime**: every table (point tables, trip-summary equivalents `flights`/`rides`/`crossing_points`
naming, and the two-point trajectory tables) has one `kdtree` index over its natural key columns,
e.g. `flight_points`: `kdtree(flight_id, t, x, y)`; `flights`: `kdtree(flight_id, t)`;
`crossings`: `kdtree(crossing_id, t, x0, y0, x1, y1)`.

## Known caveats / gaps

- SpaceTime row counts were not independently re-verified this session — the numbers in
  `/opt/stdb/spacetimed.defs` (`stats.row_count`) look like round planner-hint values (500M, 1B,
  10B), not live counts; confirm via an actual query (e.g. through `benchmark_client`'s
  `SpaceTimeExecutor`) before relying on them.
- `crossing_points` has a persistent small ClickHouse-vs-Postgres row-count gap (~0.0019–0.0009%
  across the raw table and its summary), accepted as within the same class of cross-file-duplicate
  noise documented elsewhere in COORDINATION.md — not chased further.
- The Sedona summary-table verification run logged one non-fatal
  `NoClassDefFoundError: org/apache/spark/sql/catalyst/expressions/FoldableUnevaluable` (an
  internal Spark/AQE optimizer path) — the query still returned correct, fully-matching results,
  so this looks like a harmless fallback, but is unexplained and worth a second look if it recurs
  with a real query.
- `benchmark_client-1.0-SNAPSHOT.jar` was rebuilt (`mvn -o clean install -DskipTests`, Java 17) and
  redeployed to `nst-inserter` (`~/benchmark_client_deps/`, 205 jars/495MB total) on 2026-09-22,
  picking up the `crossing_points.id` integer schema and the regenerated `cycling` resource files.
  The `_summary`-table naming fix itself (`combinedBenchConf.yaml`) does **not** appear in any
  generated/deployed resource file yet — `generateParameterVariations.py` only emits `use: true`
  queries, and all 5 fixed queries are `use: false`; regenerating the `ais` scenario the same way
  would touch only unrelated queries' random parameter values for no benefit and was skipped.
  `$SPARK_HOME=/opt/spark` (Spark 4.1.2 hadoop3, matching `nst-1` exactly) and Java 17 are set up on
  `nst-inserter`; the DuckDB Quack JDBC driver (`quack-jdbc-0.2.0-alpha.6.jar`, matching `pom.xml`)
  is present. Not yet smoke-tested from `nst-inserter` against the live cluster.
- **DistributedMobilityDB's tiling feature is installed but never invoked.** The
  `distributed_mobilitydb` extension (1.0) is built and `CREATE EXTENSION`'d on the master (see
  `server/multinode/ansible/roles/mobilitydb/tasks/main.yml`), but its actual tiling function,
  `create_spatiotemporal_distributed_table()` (README: `tiling_method` crange/hierarchical/grid,
  `tiling_type` spatial/temporal/spatiotemporal), is never called anywhere in
  `server/multinode/ansible/` — grepped repo-wide, zero hits. The `flights`/`rides`/`crossings`
  `tgeompoint` tables above are plain single-node tables, not Citus-distributed at all; what *is*
  distributed is the flat `*_points` tables, via `create_distributed_table(table, 'id')` (hash
  sharding) plus native Postgres time-range partitioning — a different, non-MD mechanism. The
  `mobilitydb_queries_space_partitioned.yaml`/`_time_partitioned.yaml` variants present for all 5
  scenarios (aviation/cycling/ais/geolife/general) reference tables like `flights_adaptgrid`/
  `rides_adapttimegrid` that look like they were written anticipating this tiling output, but none
  of those tables exist live (confirmed via `pg_tables`) — dead/aspirational config, not wired up.
