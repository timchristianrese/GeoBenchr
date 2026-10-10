# Query Catalog (snapshot: 2026-09-22)

Source of truth for every query definition: `benchmark/configuration/<scenario>/config/combinedBenchConf.yaml`.
That file is the single multi-dialect source `generateParameterVariations.py` expands into the
per-dialect, per-parameter-set YAMLs actually consumed by `benchmark_client`
(`src/main/resources/<scenario>/<dialect>_queries.yaml` and the mirrored copies under
`benchmark/configuration/<scenario>/queries/`) — edit the combined config, then regenerate, not
the generated files directly.

**"Dialects" below** lists which dialect blocks exist in the source config for that query, not
whether the query is confirmed working on that dialect — see `COORDINATION.md` for known per-query,
per-dialect bugs and fixes (ClickHouse Distributed-JOIN denial, PostGIS-only functions used in
ClickHouse blocks, malformed `BETWEEN` quoting, the `crossing_points.id` type migration, the
`region_*` lat/lon parameter bug, and others — all found and fixed this session unless noted
otherwise). `sample_postgis` is a lightweight PostGIS variant used for the general/write-heavy
query set's sampling comparison, not a 7th engine.

**60 query definitions total** across 5 scenarios: 6 general + 13 aviation + 14 cycling +
15 ais + 12 geolife. 8 are currently disabled (`use: false`):
`inCityRadiusInPeriod` (aviation), `averageRideDurationToGoAtUniversityInPeriod` +
`averageRideDurationDuringDay` (cycling), `countActiveCrossingsInPeriod` +
`averageDurationCrossingStartAtHarbourInPeriod` + `crossingsConnectTwoGivenHarbours` (ais) — the
latter 3 cycling/ais queries are the ones that reference the trip-summary tables and had stale
table/column names fixed 2026-09-22 (see [database-state.md](database-state.md)); they remain
disabled, not re-enabled, by that fix alone.

Each scenario also defines a standard set of write-operation queries (`insert*`, `update*Mass`,
`update*Single`, `delete*Mass`, `delete*Single`, `bulkInsert*`). `sedona:` blocks exist for these
(so the per-dialect YAML generation doesn't choke on a missing key) but are **left empty** —
Spark/Sedona has no live-update story in this benchmark (reads pre-exported HDFS Parquet, not a
live table); `sample_postgis` is skipped entirely for write-ops (not part of that sampling
comparison).

**DuckLake note**: `aviation`/`cycling`/`ais`/`geolife`/`general`'s `combinedBenchConf.yaml` all
carry a header comment defining what "distributed DuckDB" would mean in this benchmark: DuckLake
(a Postgres-backed catalog + Parquet on shared object storage, attached once via `ATTACH
'ducklake:...'`, then queried with plain DuckDB SQL). The comment is explicit that this is a
**shared-storage, independent-single-node-engines** model, not shared-nothing parallel query
execution — "each query still runs single-node once it reaches a DuckDB engine." **This documents
a possible future mode for how write queries should be interpreted; it is not currently deployed**
— the live `nst-1` DuckDB (see [database-state.md](database-state.md)) is a single unattached local
file, no DuckLake catalog involved.

## General (cross-scenario)

6 query definitions in `benchmark/configuration/general/config/combinedBenchConf.yaml`.

| Query | Enabled | Type | Dialects | Description |
|---|---|---|---|---|
| `SpatialBBox` | yes | spatial | clickhouse, duckdb, mobilitydb, postgis, sample_postgis, sedona, spacetime | Application-Agnostic Spatial Range Query, covering 1-5% of the dataset bounding box |
| `kNN` | yes | spatial | clickhouse, duckdb, mobilitydb, postgis, sample_postgis, sedona, spacetime | Application-Agnostic kNN Query, finding the k nearest neighbors to a given point |
| `timeInterval` | yes | temporal | clickhouse, duckdb, mobilitydb, postgis, sample_postgis, sedona, spacetime | Collect all ids within a specified time interval. |
| `timeAggregation` | yes | temporal | clickhouse, duckdb, mobilitydb, postgis, sample_postgis, sedona, spacetime | Aggregate the number of points per id within a specified time interval, grouped by hour. |
| `spatioTemporalRange` | yes | spatiotemporal | clickhouse, duckdb, mobilitydb, postgis, sample_postgis, sedona, spacetime | Get all ids within a specified time interval and spatial region. |
| `IDsInsideRegion1Andsecond_region` | yes | spatiotemporal | clickhouse, duckdb, mobilitydb, postgis, sample_postgis, sedona, spacetime | Find users who have activities in two specified regions during a given time period. |

## Aviation (flight_points)

13 query definitions in `benchmark/configuration/aviation/config/combinedBenchConf.yaml`.

| Query | Enabled | Type | Dialects | Description |
|---|---|---|---|---|
| `FlightsAtNightInPeriod` | yes | temporal | clickhouse, duckdb, mobilitydb, postgis, sample_postgis, sedona, spacetime | Find Flights that occur during nighttime hours (e.g., 10 PM to 7 AM) within a specified time period. |
| `FlightsAtLowAltitudeInPeriod` | yes | temporal | clickhouse, duckdb, mobilitydb, postgis, sample_postgis, sedona, spacetime | Find flights that fly within a specified altitude band (between a low and a high threshold) within a time period. |
| `countFlightsInCounties` | yes | spatial | clickhouse, duckdb, mobilitydb, postgis, sample_postgis, sedona, spacetime | Counts the number of distinct flights that have flown through a given county. |
| `flightClosestToPoint` | yes | spatial | clickhouse, duckdb, mobilitydb, postgis, sample_postgis, sedona, spacetime | Finds the flight that comes closest to a given point within a certain distance. |
| `flightsInCountyInPeriod` | yes | spatiotemporal | clickhouse, duckdb, mobilitydb, postgis, sample_postgis, sedona, spacetime | Finds distinct flights that have flown through a given county within a certain time period. |
| `CityActivityAtInstant` | yes | spatiotemporal | clickhouse, duckdb, mobilitydb, postgis, sample_postgis, sedona, spacetime | Find the number of distinct flights that were active in a specific city (+ radius) at a given instant in time, +- 10 seconds. |
| `inCityRadiusInPeriod` | **no** | spatiotemporal | clickhouse, duckdb, mobilitydb, postgis, sample_postgis, sedona, spacetime | Finds distinct flights that have flown within a certain radius of a given city during a specified time period. |
| `insertSingleflightPoint` | yes | insert | clickhouse, duckdb, mobilitydb, postgis, sedona, spacetime | Insert a single flight point |
| `updateflightPointMass` | yes | update | clickhouse, duckdb, mobilitydb, postgis, sedona, spacetime | Update location of every point belonging to a flight id |
| `updateflightPointSingle` | yes | update_single | clickhouse, duckdb, mobilitydb, postgis, sedona, spacetime | Update location of exactly one flight point, identified by id and timestamp |
| `deleteflightPointMass` | yes | delete | clickhouse, duckdb, mobilitydb, postgis, sedona, spacetime | Delete every point belonging to a flight id |
| `deleteflightPointSingle` | yes | delete_single | clickhouse, duckdb, mobilitydb, postgis, sedona, spacetime | Delete exactly one flight point, identified by id and timestamp |
| `bulkInsertflightPoints` | yes | bulk_insert | clickhouse, duckdb, mobilitydb, postgis, sedona, spacetime | Insert multiple points in one statement |

## Cycling (ride_points)

14 query definitions in `benchmark/configuration/cycling/config/combinedBenchConf.yaml`.

| Query | Enabled | Type | Dialects | Description |
|---|---|---|---|---|
| `RidesDuringRushHourInPeriod` | yes | temporal | clickhouse, duckdb, mobilitydb, postgis, sample_postgis, sedona, spacetime | Find rides that occur during rush hours (7-10 AM and 4-7 PM) within a specified time period. |
| `RidesAcrossMultipleHours` | yes | temporal | clickhouse, duckdb, mobilitydb, postgis, sample_postgis, sedona, spacetime | Find rides that span across multiple hours within a specified time period, i.e have ride points in more than one hour of the day. |
| `CountRidesFullyInDistrictRadius` | yes | spatial | clickhouse, duckdb, mobilitydb, postgis, sample_postgis, sedona, spacetime | Find rides that are fully contained within a specified distance of the district's geometry. |
| `countRideInDistrict` | yes | spatial | clickhouse, duckdb, mobilitydb, postgis, sample_postgis, sedona, spacetime | Counts the number of rides within a specified district. |
| `AverageUpdatesPerRideInDistrictInPeriod` | yes | spatiotemporal | clickhouse, duckdb, mobilitydb, postgis, sample_postgis, sedona, spacetime | Calculates the average number of updates (ride points) within a specified district and time period. Only the points within the district are counted, and only the rides that have at least one point in the district during the specified time period are considered. |
| `RidesPastOrToUniversityInPeriod` | yes | spatiotemporal | clickhouse, duckdb, mobilitydb, postgis, sample_postgis, sedona, spacetime | Find rides that pass by or go to a specified university within a certain distance and time period. Different to the RidesPastOrToUniversity query, this one also considers the time period and intermittent points instead of just the start and end points of the ride. |
| `averageRideDurationToGoAtUniversityInPeriod` | **no** | spatiotemporal | clickhouse, duckdb, mobilitydb, postgis, sedona, spacetime | Calculates the average duration of rides that go to a specified university within a certain distance (of the university) and time period (ride overlaps with the time period is enough as the criteria) |
| `averageRideDurationDuringDay` | **no** | temporal | clickhouse, duckdb, mobilitydb, postgis, sedona, spacetime | Calculates the average duration of rides that occur (at least partially) during a specific day. |
| `insertSingleridePoint` | yes | insert | clickhouse, duckdb, mobilitydb, postgis, sedona, spacetime | Insert a single ride point |
| `updateridePointMass` | yes | update | clickhouse, duckdb, mobilitydb, postgis, sedona, spacetime | Update location of every point belonging to a ride id |
| `updateridePointSingle` | yes | update_single | clickhouse, duckdb, mobilitydb, postgis, sedona, spacetime | Update location of exactly one ride point, identified by id and timestamp |
| `deleteridePointMass` | yes | delete | clickhouse, duckdb, mobilitydb, postgis, sedona, spacetime | Delete every point belonging to a ride id |
| `deleteridePointSingle` | yes | delete_single | clickhouse, duckdb, mobilitydb, postgis, sedona, spacetime | Delete exactly one ride point, identified by id and timestamp |
| `bulkInsertridePoints` | yes | bulk_insert | clickhouse, duckdb, mobilitydb, postgis, sedona, spacetime | Insert multiple points in one statement |

## AIS (crossing_points)

15 query definitions in `benchmark/configuration/ais/config/combinedBenchConf.yaml`.

| Query | Enabled | Type | Dialects | Description |
|---|---|---|---|---|
| `CrossingsActiveAtGivenHourOfDay` | yes | temporal | clickhouse, duckdb, mobilitydb, postgis, sedona, spacetime | Counts the number of active crossings at a given hour of the day within a time period |
| `busiestDayInWeekPeriod` | yes | temporal | clickhouse, duckdb, mobilitydb, postgis, sample_postgis, sedona, spacetime | Find the day with the highest number of unique crossings within a specified week period. |
| `countActiveCrossingsNearIsland` | yes | spatial | clickhouse, duckdb, mobilitydb, postgis, sedona, spacetime | Counts the number of active crossings near a given island within a certain distance. |
| `crossingsAroundHarbour` | yes | spatial | clickhouse, duckdb, mobilitydb, postgis, sedona, spacetime | Finds the crossings that are within a certain distance of a specified harbour. |
| `HarbourActivityInPeriod` | yes | spatiotemporal | clickhouse, duckdb, mobilitydb, postgis, sedona, spacetime | Finds the number of crossings near a specified harbour within a certain distance during a given time period. |
| `islandOrCoastLineCrossingsInPeriod` | yes | spatiotemporal | clickhouse, duckdb, mobilitydb, postgis, sedona, spacetime | For an island, find the number of crossings that are within a certain distance of the island during a specified time period. |
| `countActiveCrossingsInPeriod` | **no** | temporal | clickhouse, duckdb, mobilitydb, postgis, sedona, spacetime | Counts the number of active crossings in a given time period. |
| `averageDurationCrossingStartAtHarbourInPeriod` | **no** | spatiotemporal | clickhouse, duckdb, mobilitydb, postgis, sedona, spacetime | Calculates the average duration of crossings that start at a given harbour within a certain distance and time period. |
| `crossingsConnectTwoGivenHarbours` | **no** | spatial | clickhouse, duckdb, mobilitydb, postgis, sedona, spacetime | Finds crossings that connect two given harbours (i.e are within a specific distance of both harbours.) |
| `insertSingleCrossingPoint` | yes | insert | clickhouse, duckdb, mobilitydb, postgis, sedona, spacetime | Insert a single crossing point |
| `updateCrossingPointMass` | yes | update | clickhouse, duckdb, mobilitydb, postgis, sedona, spacetime | Update location of every point belonging to a crossing id |
| `updateCrossingPointSingle` | yes | update_single | clickhouse, duckdb, mobilitydb, postgis, sedona, spacetime | Update location of exactly one crossing point, identified by id and timestamp |
| `deleteCrossingPointMass` | yes | delete | clickhouse, duckdb, mobilitydb, postgis, sedona, spacetime | Delete every point belonging to a crossing id |
| `deleteCrossingPointSingle` | yes | delete_single | clickhouse, duckdb, mobilitydb, postgis, sedona, spacetime | Delete exactly one crossing point, identified by id and timestamp |
| `bulkInsertCrossingPoints` | yes | bulk_insert | clickhouse, duckdb, mobilitydb, postgis, sedona, spacetime | Insert multiple points in one statement |

## Geolife (geolife_points)

12 query definitions in `benchmark/configuration/geolife/config/combinedBenchConf.yaml`.

| Query | Enabled | Type | Dialects | Description |
|---|---|---|---|---|
| `FirstAndLastActivityInPeriod` | yes | temporal | clickhouse, duckdb, mobilitydb, postgis, sample_postgis, sedona, spacetime | Find the first and last activity of each user within a specified time period. This query retrieves the earliest and latest timestamps of user activities. |
| `AverageDailyActivityPerUserInPeriod` | yes | temporal | clickhouse, duckdb, mobilitydb, postgis, sample_postgis, sedona, spacetime | Find the average daily number of activities per user within a specified time period, specific to each user. |
| `ClosestRestaurantToPoint` | yes | spatial | clickhouse, duckdb, mobilitydb, postgis, sample_postgis, sedona, spacetime | Find the closest restaurant to a specified point using CheckInData. This query identifies the nearest restaurant based on spatial proximity. This query assumes that there is a restaurant within 5 km of the given point. |
| `CountPOIsByTypeInRegion` | yes | spatial | clickhouse, duckdb, mobilitydb, postgis, sample_postgis, sedona, spacetime | Count the number of points of interest (POIs) by type within a specified region. This query aggregates POIs based on their types, providing insights into the distribution of different categories of POIs within the defined spatial area. |
| `SlidingWindowActivityInRegionPerMinuteWithNonZeroID` | yes | spatiotemporal | clickhouse, duckdb, mobilitydb, postgis, sample_postgis, sedona, spacetime | Counts the number of Non-Zero Venue_ID activities in a specified region for each minute within a given time period. For every minute, the query aggregates activities occurring within a 20-minute sliding window centered on that minute (10 minutes before and after), providing a smoothed view of activity patterns over time. |
| `UniqueUsersPerHourInRegionInPeriod` | yes | spatiotemporal | clickhouse, duckdb, mobilitydb, postgis, sample_postgis, sedona, spacetime | Find the number of unique users in a specified region during a given time period, grouped by hour. This query counts distinct user IDs whose activities fall within the defined spatial area and temporal window, providing insights into user distribution and engagement. |
| `insertSinglegeolifePoint` | yes | insert | clickhouse, duckdb, mobilitydb, postgis, sedona, spacetime | Insert a single geolife point |
| `updategeolifePointMass` | yes | update | clickhouse, duckdb, mobilitydb, postgis, sedona, spacetime | Update location of every point belonging to a geolife trace id |
| `updategeolifePointSingle` | yes | update_single | clickhouse, duckdb, mobilitydb, postgis, sedona, spacetime | Update location of exactly one geolife point, identified by id and timestamp |
| `deletegeolifePointMass` | yes | delete | clickhouse, duckdb, mobilitydb, postgis, sedona, spacetime | Delete every point belonging to a geolife trace id |
| `deletegeolifePointSingle` | yes | delete_single | clickhouse, duckdb, mobilitydb, postgis, sedona, spacetime | Delete exactly one geolife point, identified by id and timestamp |
| `bulkInsertgeolifePoints` | yes | bulk_insert | clickhouse, duckdb, mobilitydb, postgis, sedona, spacetime | Insert multiple points in one statement |

