# What we add to ClickHouse (and what we add elsewhere)

State on the cluster, checked 2026-10-08 (`system.functions`, `system.columns`, `pg_proc`, `pg_extension`,
`duckdb_functions()`).

**ClickHouse is the only system where we add functions of our own.** PostGIS/MobilityDB, DuckDB, SpaceTime
and Sedona run every benchmark query with built-in or installed-extension functions only. What we add on the
other systems is data layout (indexes, summary tables, trajectory tables) — listed at the end for comparison.

## 1. SQL functions we define in ClickHouse

All are SQL lambda functions (`CREATE FUNCTION name AS (args) -> expression`): ClickHouse expands them into
the query text before running it, so they cost exactly what the written-out expression costs. Definitions:
`configuration/scripts/clickhouse_functions.sql`, created on all five nodes (`ON CLUSTER cluster_1`).

### Used by the benchmark queries: H3 cell covers

The point tables are sorted by `h3` (H3 cell, resolution 9). A query that filters `h3 GLOBAL IN (<cells>)`
lets ClickHouse skip every part of the table outside those cells — ClickHouse's spatial index. ClickHouse
has no built-in "cells within distance d of this shape", so we build the cell list ourselves. The list is
deliberately a superset; the query's exact spatial condition is unchanged and decides the result.

| Function | What it computes | Used by |
|---|---|---|
| `h3DensifyPath(path)` | the points of a lon/lat path with one point at least every 0.005° (so no stretch of outline falls between two sample points) | `h3CoverRing`, `h3CoverLine` |
| `h3CoverCells(seeds, d)` | res-7 seed cells → widened by `ceil(d / 1000) + 2` rings (`h3kRing`) → their res-9 children (`h3ToChildren`) | the three below |
| `h3CoverPoint(lon, lat, d)` | res-9 cells within distance `d` of a point | harbours and cities (`crossingsAroundHarbour`, `HarbourActivityInPeriod`, `CityActivityAtInstant`), `flightClosestToPoint`, `ClosestRestaurantToPoint`, `kNN` (all scenarios) |
| `h3CoverRing(ring, d)` | res-9 cells inside, or within `d` of, a polygon: cells whose centre is inside (`h3PolygonToCells`) plus the cells of the densified outline, widened by `d` | rectangles (`SpatialBBox`, `spatioTemporalRange`, `IDsInsideRegion1Andsecond_region`, geolife region queries), districts, counties, universities, islands |
| `h3CoverLine(line, d)` | res-9 cells within `d` of an open line | coastlines (`countActiveCrossingsNearIsland`, `islandOrCoastLineCrossingsInPeriod`) |

31 of the 48 benchmark queries (from 19 query templates; the general ones run in all four scenarios) carry
such an `h3 GLOBAL IN (...)` filter;
the previous scan-only versions are kept in the configs as `clickhouse_scan` (not run). On all 5 parameter
sets of every query the H3 and scan-only versions return identical results (149 of 155 pairs byte-identical;
the other 6 are nearest-neighbour ties at exactly equal distance).

### Defined but not used by any benchmark query any more

| Function | Definition | Where it still appears |
|---|---|---|
| `webMercatorX(lon)` | `6378137 · radians(lon)` (EPSG:3857 x) | consistency check of the stored `x` column; 3 disabled summary-table queries |
| `webMercatorY(lat)` | `6378137 · ln(tan(π/4 + radians(lat)/2))` (EPSG:3857 y) | same |
| `webMercatorDistance(lon1, lat1, lon2, lat2)` | planar EPSG:3857 distance | 3 disabled queries |

These projected every row at query time until 2026-10-07; the queries now use the stored `x`/`y` columns.
ClickHouse's own distance functions (`greatCircleDistance`, `geoDistance`) measure on the sphere/ellipsoid,
not PostGIS's planar EPSG:3857 distance, so they would not select the same points.

## 2. Stored columns and indexes in ClickHouse

| Table(s) | Addition | Why |
|---|---|---|
| the four point tables | `h3 = geoToH3(lat, lon, 9)`, first column of the sort key `(h3, timestamp, id)` | the spatial index the H3 covers use |
| the four point tables, `harbours`, `cities` | `x`, `y` = EPSG:3857 projection of lon/lat (`MATERIALIZED`, computed once per row on insert) | PostGIS stores geometries already projected to EPSG:3857; with `x`/`y` a ClickHouse distance check is the same plain arithmetic on stored values |
| the four point tables | `Point = (lon, lat)` | tuple form from the original setup; the queries pass `(lon, lat)` directly |
| the four point tables | `minmax` skip indexes on `lon`, `lat`, `timestamp` | lets `BETWEEN` filters skip blocks |
| trip summary tables | `start_h3`, `start_point`, `end_point` | same layout for the summary tables |

## 3. Logic written out inside individual queries

No function is involved here, but the query does work that is a single function call elsewhere:

| Where | What | Elsewhere |
|---|---|---|
| island/coastline queries | distance from a point to a coastline: minimum over the line's segments of the point-to-segment distance, with `arrayMap` (ClickHouse's `polygonsDistanceCartesian` only handles polygons; feeding it the line closed it into an area — fixed 2026-10-08) | PostGIS/DuckDB/Sedona `ST_DWithin`, SpaceTime `ST_DWithin` |
| island/coastline/university queries | polygon vertices projected to EPSG:3857 inline (`6378137 · radians(…)`) before `polygonsDistanceCartesian` | geometries stored in EPSG:3857 |
| aviation/AIS time literals | `toDateTime(…, 'UTC')` | `AT TIME ZONE 'UTC'` / naive UTC timestamps |

## 4. For comparison: what we add on the other systems

| System | Functions of our own | Data layout we built |
|---|---|---|
| PostGIS (Citus) | none (extensions: Citus, PostGIS) | GiST on `geom`, B-trees on `id`, `timestamp`, `(id, timestamp)` (+ `altitude`, `venue_id`, `rider_id`); trip summary tables with GiST/B-tree indexes |
| MobilityDB (Citus) | none (extensions: MobilityDB, Distributed MobilityDB) | trajectory tables `crossings`/`flights`/`rides` (discrete `tgeompoint`, range-distributed tiles) with GiST on `trip` |
| DuckDB | none (spatial extension) | R-tree indexes on the reference tables and the flight/ride summary tables; point tables sorted by time bucket (month/day/hour), then Hilbert curve, so the per-row-group geometry min/max lets `ST_Intersects_Extent` (built-in) skip row groups |
| SpaceTime | none (partner-operated) | partner's own layout |
| Sedona | none (Sedona built-ins) | Parquet on HDFS (replication 2), registered once per session; `flight_points`, `ride_points`, `geolife_points` as GeoParquet with a `geom_bbox` covering column, sorted by geohash within each file, so row-group statistics prune spatial filters |

All systems answer the same trip-span questions on equivalent trip summary tables (`*_summary` in PostGIS,
DuckDB, ClickHouse and HDFS; SpaceTime's own `rides`/`flights`/`crossings`).
