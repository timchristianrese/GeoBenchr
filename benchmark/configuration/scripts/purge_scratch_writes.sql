-- Cleanup for the write benchmarks (insert/update/delete/bulk_insert) after one or more repeated
-- experiment runs, without reloading the dataset.
--
-- generateParameterVariations.py (aviation/cycling/geolife/ais) now mints ids for these queries
-- from a negative id space (ScratchIdPool, see get_scratch_id()) instead of the old
-- rnd.randint(1_000_000_000, 2_000_000_000) band -- that band already collided with real data
-- (crossing_id on the live cluster runs up to ~13.05B). Every row a write benchmark run has ever
-- inserted therefore has id < 0, and every real row has id >= 0, so resetting to a clean baseline
-- for the next experiment is one indexed delete per table -- no reload required.
--
-- Run this against Postgres (postgis and mobilitydb both write through the same flat
-- "<x>_points(id, geom, timestamp)" tables there). Sedona doesn't execute these write queries at
-- all (no live-update support, see combinedBenchConf.yaml), so it needs no cleanup.
DELETE FROM flight_points    WHERE id < 0;
DELETE FROM ride_points      WHERE id < 0;
DELETE FROM geolife_points   WHERE id < 0;
DELETE FROM crossing_points  WHERE id < 0;

-- DuckDB: used to be a live view over the same Postgres tables above (so the DELETEs there also
-- covered it) -- it now has its own materialized copy (see roles/duckdb's "Materialize GeoBenchr
-- tables" task), so its write-benchmark scratch rows need their own purge, run against the Quack
-- endpoint (jdbc:quack://<host>:9494/) rather than Postgres.
-- DELETE FROM flight_points    WHERE id < 0;
-- DELETE FROM ride_points      WHERE id < 0;
-- DELETE FROM geolife_points   WHERE id < 0;
-- DELETE FROM crossing_points  WHERE id < 0;

-- ClickHouse: a plain DELETE isn't valid against a Distributed-engine table (confirmed live, see
-- the FIX comments on aviation's updateflightPoint/deleteflightPoint) -- mutations have to target
-- the underlying "_local" table, on every shard (ON CLUSTER). aviation's cluster/table names are
-- confirmed from that fix; cycling/geolife/ais haven't had the same ClickHouse write queries
-- fixed yet (their clickhouse: blocks for update/delete are still the plain, untested form), so
-- fill in their real _local table + cluster names here once that happens -- don't assume they
-- match aviation's without checking.
ALTER TABLE flight_points_local ON CLUSTER cluster_1 DELETE WHERE id < 0;
-- ALTER TABLE ride_points_local     ON CLUSTER cluster_1 DELETE WHERE id < 0;
-- ALTER TABLE geolife_points_local  ON CLUSTER cluster_1 DELETE WHERE id < 0;
-- ALTER TABLE crossing_points_local ON CLUSTER cluster_1 DELETE WHERE id < 0;

-- SpaceTime (the nst-1..5 cluster): insert/update/delete/bulk_insert are still unimplemented
-- there (see combinedBenchConf.yaml's blank spacetime: fields for every write query -- the SpaceTime
-- team tracks this as T21540), so nothing to purge on that engine yet. When those land, note
-- SpaceTime's own tables key on flight_id/crossing_id/etc., not a uniform "id" column -- this
-- script's WHERE clauses won't apply as-is.
