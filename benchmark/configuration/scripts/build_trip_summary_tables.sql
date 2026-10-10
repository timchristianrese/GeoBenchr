-- Trip-wise summary (trajectory) tables for MobilityDB: "flights" (aviation), "rides" (cycling),
-- "crossings" (ais). Built from the point tables the benchmark already has, so no new raw data
-- is needed. Fields required were determined by grep'ing the *active* (use: true) mobilitydb:
-- queries in each dataset's combinedBenchConf.yaml for which columns they actually reference:
--   flights:   id, trip (tgeompoint), altitude (tfloat)
--   rides:     id, trip (tgeompoint)
--   crossings: id, trip (tgeompoint)  -- note c.id > 0 filters used throughout: id=0 looks like a
--              placeholder/sentinel row in crossing_points, exclude it from the aggregate too.
--
-- NOT YET RUN LIVE -- the connection-pool exhaustion on the Citus workers (orphaned
-- citus_internal backends from killed load scripts, ~94-101 of each worker's 100 max_connections
-- already consumed at idle) blocked verifying this against the real MobilityDB version's exact
-- temporal-constructor function names before this had to be handed off. Two known-name
-- conventions exist across MobilityDB versions -- try tgeompointSeq/tfloatSeq (1.1+) first; if
-- "function does not exist", MobilityDB 1.0.x used tgeompointSeqSet/tfloatSeqSet.
--
-- FIX (optimization -- watch for long-running full-table aggregates): a plain
-- "GROUP BY id" over a several-hundred-million-to-billion-row point table forces one massive
-- sort+aggregate pass and holds every trip's full point array in memory until its group closes.
-- Batching by id range keeps memory bounded and lets you checkpoint progress (safe to stop/resume
-- between batches, unlike one giant transaction) -- adjust BATCH_SIZE/max id to the real id range
-- (see each table's own MAX(id) once you have a live connection; this defaults to a wide bound
-- and just does nothing for empty ranges).

DO $$
DECLARE
    batch_size BIGINT := 5000000;  -- tune based on observed memory/time per batch
    lo BIGINT;
    max_id BIGINT;
BEGIN
    -- ===== flights (aviation) =====
    CREATE TABLE IF NOT EXISTS flights (
        id BIGINT PRIMARY KEY,
        trip tgeompoint,
        altitude tfloat
    );
    SELECT COALESCE(MAX(id), 0) INTO max_id FROM flight_points;
    lo := 0;
    WHILE lo <= max_id LOOP
        INSERT INTO flights (id, trip, altitude)
        SELECT
            id,
            tgeompointSeq(array_agg(tgeompoint(geom, timestamp) ORDER BY timestamp)),
            tfloatSeq(array_agg(tfloat(altitude, timestamp) ORDER BY timestamp))
        FROM flight_points
        WHERE id >= lo AND id < lo + batch_size
        GROUP BY id
        ON CONFLICT (id) DO NOTHING;
        lo := lo + batch_size;
    END LOOP;

    -- ===== rides (cycling) =====
    CREATE TABLE IF NOT EXISTS rides (
        id BIGINT PRIMARY KEY,
        trip tgeompoint
    );
    SELECT COALESCE(MAX(id), 0) INTO max_id FROM ride_points;
    lo := 0;
    WHILE lo <= max_id LOOP
        INSERT INTO rides (id, trip)
        SELECT
            id,
            tgeompointSeq(array_agg(tgeompoint(geom, timestamp) ORDER BY timestamp))
        FROM ride_points
        WHERE id >= lo AND id < lo + batch_size
        GROUP BY id
        ON CONFLICT (id) DO NOTHING;
        lo := lo + batch_size;
    END LOOP;

    -- ===== crossings (ais) =====
    CREATE TABLE IF NOT EXISTS crossings (
        id BIGINT PRIMARY KEY,
        trip tgeompoint
    );
    -- crossing_points.id is text (see FIX note in purge_scratch_writes.sql / sedona export
    -- config -- confirmed live via information_schema.columns), so cast for the range loop and
    -- for the "id > 0" placeholder-row exclusion every active ais query already applies.
    SELECT COALESCE(MAX(id::bigint), 0) INTO max_id FROM crossing_points WHERE id ~ '^[0-9]+$';
    lo := 0;
    WHILE lo <= max_id LOOP
        INSERT INTO crossings (id, trip)
        SELECT
            id::bigint,
            tgeompointSeq(array_agg(tgeompoint(geom, timestamp) ORDER BY timestamp))
        FROM crossing_points
        WHERE id ~ '^[0-9]+$' AND id::bigint >= lo AND id::bigint < lo + batch_size AND id::bigint > 0
        GROUP BY id::bigint
        ON CONFLICT (id) DO NOTHING;
        lo := lo + batch_size;
    END LOOP;
END $$;

CREATE INDEX IF NOT EXISTS flights_trip_idx ON flights USING gist (trip);
CREATE INDEX IF NOT EXISTS rides_trip_idx ON rides USING gist (trip);
CREATE INDEX IF NOT EXISTS crossings_trip_idx ON crossings USING gist (trip);
