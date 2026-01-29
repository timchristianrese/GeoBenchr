#!/bin/bash
system=$1
application=$2
size=$3
format=$4
# --- Subset parameter setup ---
# Accepted sizes: full, 1M, 10M, 100M, 1B
case "$size" in
    full)
        TARGET_POINTS=0
        ;;
    1M)
        TARGET_POINTS=1000000
        ;;
    10M)
        TARGET_POINTS=10000000
        ;;
    100M)
        TARGET_POINTS=100000000
        ;;
    1B)
        TARGET_POINTS=1000000000
        ;;
    *)
        echo "⚠️  Unknown size '$size'. Use one of: full, 1M, 10M, 100M, 1B"
        exit 1
        ;;
esac
echo "Selected dataset size: $size ($TARGET_POINTS points)"

if [ "$system" == "all" ]; then
    echo "Loading data into mobilitydb"
# Set PostgreSQL connection parameters
    DB_NAME="postgres"
    DB_USER="postgres"
    DB_HOST="localhost"
    DB_PASSWORD="test"
    export PGPASSWORD=$DB_PASSWORD
    echo $application
    if [ "$application" == "ais" ]; then
        psql -U "$DB_USER" -d "$DB_NAME" -h "$DB_HOST" <<EOF
        -- Enable required extensions
        CREATE EXTENSION IF NOT EXISTS postgis;
        CREATE EXTENSION IF NOT EXISTS mobilitydb;
        CREATE EXTENSION IF NOT EXISTS timescaledb;
        DROP TABLE IF EXISTS crossings;
        DROP TABLE IF EXISTS crossings_AdaptGrid;
        DROP TABLE IF EXISTS crossings_AdaptTimeGrid;
        DROP TABLE IF EXISTS AllPoints;
        DROP TABLE IF EXISTS XBins;
        DROP TABLE IF EXISTS YBins;
        DROP TABLE IF EXISTS crossing_trajectories;
        DROP TABLE IF EXISTS crossing_points;
        DROP TABLE IF EXISTS tsdb_crossing_points;
        DROP TABLE IF EXISTS tsdb_crossing_trajectories;
        DROP TABLE IF EXISTS harbours;
        DROP TABLE IF EXISTS islands;
        CREATE TABLE crossings (
            crossing_id integer, 
            trip tgeompoint
        );
        CREATE TABLE crossing_points (
            crossing_id integer,
            timestamp timestamp,
            vessel_id Text,
            geom Geometry(Point, 4326),
            heading float,
            speed float,
            course float
        );
        CREATE TABLE harbours (
            name TEXT,
            geom Geometry(Point, 4326),
            type Text
        );
        CREATE TABLE islands (
            name TEXT,
            geom Geometry(MultiPolygon, 4326)
        );

        COPY islands FROM '/home/tim/data/ais/resources/islands-wkt-simplified.csv' WITH (FORMAT csv, HEADER true, DELIMITER ';');
        -- harbours has name, lat, lon, type in csv, convert to geom
        CREATE TABLE IF NOT EXISTS temp_harbour_raw (
            name TEXT,
            lat TEXT,
            lon TEXT,
            type TEXT
        );
        \copy temp_harbour_raw(name, lat, lon, type) FROM '/home/tim/data/ais/resources/harbours.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',');
        -- Insert parsed WKT into final table
        INSERT INTO harbours (name, geom, type)
        SELECT name, 
('POINT(' || lon || ' ' || lat || ')'), type FROM temp_harbour_raw;
        DROP TABLE IF EXISTS temp_harbour_raw;
EOF
for file in /home/tim/data/ais/point*.csv; do
    echo "Processing $file..."
    psql -U "$DB_USER" -d "$DB_NAME" -h "$DB_HOST" <<EOF
    DROP TABLE IF EXISTS temp_crossing_raw;
    CREATE TEMP TABLE temp_crossing_raw (
        crossing_id integer,
        timestamp TIMESTAMP,
        vessel_id TEXT,
        geom_txt TEXT, 
        heading TEXT,
        speed TEXT,
        course TEXT
    );
    \copy temp_crossing_raw FROM '$file' WITH (FORMAT csv, DELIMITER ',', HEADER True)
    INSERT INTO crossing_points (crossing_id, timestamp, vessel_id, geom, heading, speed, course)
    SELECT
        crossing_id,
        timestamp,
        vessel_id,
        ST_GeomFromText(geom_txt, 4326),
        heading::float,
        speed::float,
        course::float
    FROM temp_crossing_raw;
EOF
done
    #delete all single row trips
    psql -U "$DB_USER" -d "$DB_NAME" -h "$DB_HOST" <<EOF
    DELETE FROM crossing_points
    WHERE crossing_id IN (
        SELECT crossing_id
        FROM crossing_points
        GROUP BY crossing_id
        HAVING COUNT(*) = 1
    );
EOF
# --- Subset AIS trajectories if requested ---
if [ "$TARGET_POINTS" -gt 0 ]; then
    echo "Subsetting AIS dataset to approximately $size ($TARGET_POINTS points)..."
    psql -U "$DB_USER" -d "$DB_NAME" -h "$DB_HOST" <<EOF
    DROP TABLE IF EXISTS trip_sizes;
    CREATE TEMP TABLE trip_sizes AS
    SELECT crossing_id AS trip_id, COUNT(*) AS n_points
    FROM crossing_points
    GROUP BY crossing_id;

    WITH cumulative AS (
        SELECT trip_id, n_points,
               SUM(n_points) OVER (ORDER BY random()) AS cum_sum
        FROM trip_sizes
    )
    SELECT trip_id INTO TEMP TABLE sampled_trajectories
    FROM cumulative
    WHERE cum_sum <= $TARGET_POINTS;

    DELETE FROM crossing_points
    WHERE crossing_id NOT IN (SELECT trip_id FROM sampled_trajectories);

    ANALYZE crossing_points;

    ALTER TABLE crossing_points ALTER COLUMN geom TYPE Geometry(Point, 3857) USING ST_Transform(geom, 3857);
    ALTER TABLE harbours ALTER COLUMN geom TYPE Geometry(Point, 3857) USING ST_Transform(geom, 3857);
    ALTER TABLE islands ALTER COLUMN geom TYPE Geometry(MultiPolygon, 3857) USING ST_Transform(geom, 3857);

    CREATE INDEX IF NOT EXISTS idx_crossing_points_crossing_id_ts 
    ON crossing_points (crossing_id, timestamp);
   
        WITH params AS (
    SELECT 5000::int AS chunk_size
    ),

    -- Step 1: Identify all crossings larger than the chunk size
    large_crossings AS (
    SELECT crossing_id
    FROM crossing_points, params
    GROUP BY crossing_id
    HAVING COUNT(*) > (SELECT chunk_size FROM params)
    ),

    -- Step 2: Assign a chunk number to each row
    numbered AS (
    SELECT
        cp.ctid,
        cp.crossing_id,
        ROW_NUMBER() OVER (PARTITION BY cp.crossing_id ORDER BY cp.timestamp) AS rn,
        p.chunk_size
    FROM crossing_points cp
    JOIN large_crossings lc ON cp.crossing_id = lc.crossing_id
    JOIN params p ON true
    ),

    -- Step 3: Compute the new crossing_id
    to_update AS (
    SELECT
        n.ctid,
        -- Ensure unique IDs by using a high offset based on original crossing_id
        n.crossing_id * 10000 + FLOOR((n.rn - 1) / n.chunk_size)::int AS new_crossing_id
    FROM numbered n
    )

    -- Step 4: Update crossing_ids
    UPDATE crossing_points AS cp
    SET crossing_id = u.new_crossing_id
    FROM to_update AS u
    WHERE cp.ctid = u.ctid;

EOF
fi
psql -U "$DB_USER" -d "$DB_NAME" -h "$DB_HOST" <<EOF
WITH deduplicated_points AS (
    SELECT DISTINCT ON (crossing_id, timestamp)
        crossing_id,
        timestamp,
        vessel_id,
        geom,
        heading,
        speed,
        course
    FROM crossing_points
    ORDER BY crossing_id, timestamp
)
INSERT INTO crossings (crossing_id, trip)
SELECT 
    crossing_id,
    tgeompointseq(
        array_agg(tgeompoint(geom, timestamp) ORDER BY timestamp),
        'discrete'
    )
FROM deduplicated_points
GROUP BY crossing_id;

-- time partitioning 
DROP TABLE IF EXISTS crossings_AdaptTimeGrid;
DROP TABLE IF EXISTS AdaptiveBins;

CREATE TABLE AdaptiveBins(BinId, Period) AS
        WITH Times(T) AS (
        SELECT unnest(timestamps(trip))
        FROM crossings
        ORDER BY 1 ),
        MaxTime(MaxT) AS (
        SELECT MAX(T) FROM Times ),
        Bins1(BinId, T) AS (
        SELECT NTILE(4) OVER(ORDER BY T), T
        FROM Times ),
        Bins2(BinId, T, RowNo) AS (
        SELECT BinId, T, ROW_NUMBER() OVER (PARTITION BY BinId ORDER BY T)
        FROM Bins1 )
        SELECT BinId, span(T, COALESCE(LEAD(T, 1) OVER (ORDER BY T), MaxT))
        FROM Bins2, MaxTime
        WHERE RowNo = 1;


CREATE TABLE crossings_AdaptTimeGrid(LIKE crossings, TileId text)
PARTITION BY LIST(TileId);
CREATE TABLE crossings_AdaptTimeGrid_1 PARTITION OF crossings_AdaptTimeGrid
FOR VALUES IN (1);
CREATE TABLE crossings_AdaptTimeGrid_2 PARTITION OF crossings_AdaptTimeGrid
FOR VALUES IN (2);
CREATE TABLE crossings_AdaptTimeGrid_3 PARTITION OF crossings_AdaptTimeGrid
FOR VALUES IN (3);
CREATE TABLE crossings_AdaptTimeGrid_4 PARTITION OF crossings_AdaptTimeGrid
FOR VALUES IN (4);

INSERT INTO crossings_AdaptTimeGrid(crossing_id, trip, TileId)
SELECT c.crossing_id, c.trip, ab.BinId
FROM crossings c, AdaptiveBins ab
WHERE atTime(c.trip, Period) IS NOT NULL;

-- space partitioning
DROP TABLE IF EXISTS allpoints;
DROP TABLE IF EXISTS XBins;
DROP TABLE IF EXISTS YBins;
DROP TABLE IF EXISTS AdaptiveGrid;
DROP TABLE IF EXISTS crossings_AdaptGrid;


CREATE TABLE AllPoints(Point) AS
SELECT getValue(unnest(instants(trip)))
FROM crossings;


CREATE TABLE XBins(XBinId, SpanX) AS
WITH Bins1(BinId, X) AS (
SELECT NTILE(2) OVER(ORDER BY ST_X(Point)), ST_X(Point)
FROM AllPoints ),
Bins2(BinId, X, RowNo) AS (
SELECT BinId, X, ROW_NUMBER() OVER (PARTITION BY BinId ORDER BY X)
FROM Bins1 )
SELECT BinId, span(X, COALESCE(LEAD(X, 1) OVER (ORDER BY X),
(SELECT MAX(ST_X(Point)) FROM AllPoints)))
FROM Bins2
WHERE RowNo = 1;

CREATE TABLE YBins(YBinId, SpanY) AS
WITH Bins1(BinId, Y) AS (
SELECT NTILE(2) OVER(ORDER BY ST_Y(Point)), ST_Y(Point)
FROM AllPoints ),
Bins2(BinId, Y, RowNo) AS (
SELECT BinId, Y, ROW_NUMBER() OVER (PARTITION BY BinId ORDER BY Y)
FROM Bins1 )
SELECT BinId, span(Y, COALESCE(LEAD(Y, 1) OVER (ORDER BY Y),
(SELECT MAX(ST_Y(Point)) FROM AllPoints)))
FROM Bins2
WHERE RowNo = 1;

CREATE TABLE AdaptiveGrid(TileId, RowNo, ColNo, Tile, Geom) AS
WITH TableSRID(SRID) AS (
SELECT ST_SRID(Point) FROM AllPoints LIMIT 1 ),
Tiles(TileId, RowNo, ColNo, Tile) AS (
SELECT ROW_NUMBER() OVER(), YBinId, XBinId,
stboxX(lower(SpanX), lower(SpanY), upper(SpanX), upper(SpanY), SRID)
FROM YBins y, XBins x, TableSRID s
ORDER BY YBinId, XBinId )
SELECT TileId, RowNo, ColNo, Tile, Tile::geometry
FROM Tiles;

CREATE TABLE crossings_AdaptGrid(LIKE crossings, TileId text)
PARTITION BY LIST(TileId);

CREATE TABLE crossings_AdaptGrid_1 PARTITION OF crossings_AdaptGrid
FOR VALUES IN (1);
CREATE TABLE crossings_AdaptGrid_2 PARTITION OF crossings_AdaptGrid
FOR VALUES IN (2);
CREATE TABLE crossings_AdaptGrid_3 PARTITION OF crossings_AdaptGrid
FOR VALUES IN (3);
CREATE TABLE crossings_AdaptGrid_4 PARTITION OF crossings_AdaptGrid
FOR VALUES IN (4);


INSERT INTO crossings_AdaptGrid(crossing_id, trip, TileId)
SELECT c.crossing_id, c.trip, ag.TileId
FROM crossings c, AdaptiveGrid ag
WHERE atStbox(c.trip, ag.Tile, false) IS NOT NULL;

CREATE TABLE IF NOT EXISTS tsdb_crossing_points as 
SELECT 
    crossing_id,
    timestamp,
    vessel_id,
    geom,
    heading,
    speed,
    course
FROM crossing_points;

SELECT create_hypertable('tsdb_crossing_points', 'timestamp', chunk_time_interval => interval '1 day', if_not_exists => TRUE, migrate_data => TRUE);



CREATE TABLE IF NOT EXISTS crossing_trajectories AS
SELECT 
    crossing_id, 
    startValue(trip) AS origin_geom,
    endValue(trip) AS destination_geom,
    startTimestamp(trip) AS start_time,
    endTimestamp(trip) AS end_time
FROM crossings;

CREATE TABLE IF NOT EXISTS tsdb_crossing_trajectories AS
SELECT 
    crossing_id, 
    origin_geom,
    destination_geom,
    start_time,
    end_time
FROM crossing_trajectories;



EOF

    psql -U "$DB_USER" -d "$DB_NAME" -h "$DB_HOST" <<EOF
    CREATE INDEX IF NOT EXISTS idx_harbours_geom ON harbours USING gist (geom);
    CREATE INDEX IF NOT EXISTS idx_islands_geom ON islands USING gist (geom);
    
    CREATE INDEX ON crossing_trajectories USING GIST (origin_geom);
    CREATE INDEX ON crossing_trajectories USING GIST (destination_geom);
    CREATE INDEX ON crossing_trajectories(start_time);
    CREATE INDEX ON crossing_trajectories(end_time);

    CREATE INDEX ON tsdb_crossing_trajectories USING GIST (origin_geom);
    CREATE INDEX ON tsdb_crossing_trajectories USING GIST (destination_geom);
    CREATE INDEX ON tsdb_crossing_trajectories(start_time);
    CREATE INDEX ON tsdb_crossing_trajectories(end_time);


    CREATE INDEX IF NOT EXISTS idx_crossing_points_geom ON crossing_points USING gist (geom);
    CREATE INDEX IF NOT EXISTS idx_crossing_points_timestamp ON crossing_points (timestamp);

    CREATE INDEX IF NOT EXISTS idx_crossings_geom ON crossings USING gist (trip);

    CREATE INDEX IF NOT EXISTS idx_tsdb_crossing_points_geom ON tsdb_crossing_points USING gist (geom);
    CREATE INDEX IF NOT EXISTS idx_tsdb_crossing_points_timestamp ON tsdb_crossing_points (timestamp);

    

    CREATE INDEX IF NOT EXISTS idx_crossings_adapttimegrid_geom ON crossings_AdaptTimeGrid USING gist (trip);
    -- create on partitions
    CREATE INDEX IF NOT EXISTS idx_crossings_adapttimegrid_1_geom ON crossings_AdaptTimeGrid_1 USING gist (trip);
    CREATE INDEX IF NOT EXISTS idx_crossings_adapttimegrid_2_geom ON crossings_AdaptTimeGrid_2 USING gist (trip);
    CREATE INDEX IF NOT EXISTS idx_crossings_adapttimegrid_3_geom ON crossings_AdaptTimeGrid_3 USING gist (trip);
    CREATE INDEX IF NOT EXISTS idx_crossings_adapttimegrid_4_geom ON crossings_AdaptTimeGrid_4 USING gist (trip);



    CREATE INDEX IF NOT EXISTS idx_crossings_adaptgrid_geom ON crossings_AdaptGrid USING gist (trip);
    -- create on partitions
    CREATE INDEX IF NOT EXISTS idx_crossings_adaptgrid_1_geom ON crossings_AdaptGrid_1 USING gist (trip);
    CREATE INDEX IF NOT EXISTS idx_crossings_adaptgrid_2_geom ON crossings_AdaptGrid_2 USING gist (trip);
    CREATE INDEX IF NOT EXISTS idx_crossings_adaptgrid_3_geom ON crossings_AdaptGrid_3 USING gist (trip);
    CREATE INDEX IF NOT EXISTS idx_crossings_adaptgrid_4_geom ON crossings_AdaptGrid_4 USING gist (trip);






EOF
    elif [ "$application" == "aviation" ]; then
        psql -U "$DB_USER" -d "$DB_NAME" -h "$DB_HOST" <<EOF
        -- Enable required extensions
        CREATE EXTENSION IF NOT EXISTS postgis;
        CREATE EXTENSION IF NOT EXISTS mobilitydb;
        CREATE EXTENSION IF NOT EXISTS timescaledb;
        DROP TABLE IF EXISTS flight_points;
        DROP TABLE IF EXISTS flight_trajectories;
        DROP TABLE IF EXISTS flights;
        DROP TABLE IF EXISTS flights_AdaptGrid;
        DROP TABLE IF EXISTS flights_AdaptTimeGrid;
        DROP TABLE IF EXISTS tsdb_flight_points;
        DROP TABLE IF EXISTS tsdb_flight_trajectories;
        DROP TABLE IF EXISTS airports;
        DROP TABLE IF EXISTS cities;
        DROP TABLE IF EXISTS counties;
        DROP TABLE IF EXISTS districts;
        DROP TABLE IF EXISTS municipalities;
        CREATE TABLE flight_points (
            flightid       BIGINT,
            airplanetype  TEXT,
            origin         TEXT,
            destination    TEXT,
            geom           Geometry(Point, 4326),
            timestamp            TIMESTAMP,
            altitude       FLOAT
        );

        CREATE TABLE IF NOT EXISTS flights (
            flightid BIGINT,
            airplanetype TEXT,
            origin TEXT,
            destination TEXT,
            altitude tfloat,
            trip tgeompoint
        );

        CREATE TABLE airports (
            iata       TEXT,
            icao       TEXT,
            name       TEXT,
            country    TEXT,
            city       TEXT
        );

        CREATE TABLE cities (
            area        FLOAT,
            latitude    FLOAT,
            longitude   FLOAT,
            district    TEXT,
            name        TEXT,
            population  INTEGER,
            geom        Geometry(Point, 3857) GENERATED ALWAYS AS (
                ST_Transform(ST_SetSRID(ST_MakePoint(longitude, latitude), 4326), 3857)
            ) STORED
        );
        CREATE TABLE counties (
            name     TEXT,
            geom  Geometry(Polygon, 4326)
        );

        CREATE TABLE districts (
            name     TEXT,
            geom  Geometry(Polygon, 4326)
        );

        CREATE TABLE municipalities (
            name     TEXT,
            geom  Geometry(Polygon, 4326)
        );

        CREATE TEMP TABLE cities_raw (
        area double precision,
        latitude double precision,
        longitude double precision,
        district text,
        name text,
        population integer
        );

        \copy cities_raw FROM '/home/tim/data/aviation/resources/cities.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',');

        INSERT INTO cities (area, latitude, longitude, district, name, population)
        SELECT area, latitude, longitude, district, name, population FROM cities_raw;
        DROP TABLE IF EXISTS cities_raw;
EOF
        

        for table in counties districts municipalities; do
            echo "Loading $table..."
            psql -U "$DB_USER" -d "$DB_NAME" -h "$DB_HOST" <<EOF
        CREATE TABLE IF NOT EXISTS temp_${table}_raw (
            name TEXT,
            wkt TEXT
        );
        \copy temp_${table}_raw(name, wkt) FROM '/home/tim/data/aviation/resources/${table}-wkt-simplified.csv' WITH (FORMAT csv, DELIMITER ';', HEADER true)

        -- Insert parsed WKT into final table
        INSERT INTO ${table} (name, geom)
        SELECT name, 
        ST_GeomFromText(wkt, 4326)
        FROM temp_${table}_raw;

        DROP TABLE IF EXISTS temp_${table}_raw;
EOF
        done
#         done
        #insert cities
        
        echo "Loading flight data..."
        for file in /home/tim/data/aviation/point*.csv; do
            echo "Processing $file..."
            psql -U "$DB_USER" -d "$DB_NAME" -h "$DB_HOST" <<EOF
        -- Use a temp table to parse and cast the input
        DROP TABLE IF EXISTS temp_flight_raw;
        CREATE TEMP TABLE temp_flight_raw (
            flightid BIGINT,
            airplanetype TEXT,
            origin TEXT,
            destination TEXT,
            geom_txt TEXT,
            timestamp TIMESTAMP,
            altitude FLOAT
        );

        \copy temp_flight_raw FROM '$file' WITH (FORMAT csv, DELIMITER ';', HEADER false)
        INSERT INTO flight_points (flightid, airplanetype, origin, destination, geom, timestamp, altitude)
        SELECT
            flightid,
            airplanetype,
            origin,
            destination,
            ST_GeomFromText(geom_txt, 4326),
            timestamp,
            altitude
        FROM temp_flight_raw;

EOF
        done
    #delete all single row trips
    psql -U "$DB_USER" -d "$DB_NAME" -h "$DB_HOST" <<EOF
    DELETE FROM flight_points
    WHERE flightid IN (
        SELECT flightid
        FROM flight_points
        GROUP BY flightid
        HAVING COUNT(*) = 1
    );
EOF
        # --- Subset Aviation trajectories if requested ---
if [ "$TARGET_POINTS" -gt 0 ]; then
    echo "Subsetting Aviation dataset to approximately $size ($TARGET_POINTS points)..."
    psql -v ON_ERROR_STOP=1 -U "$DB_USER" -d "$DB_NAME" -h "$DB_HOST" <<EOF
    -- ==========================================
    -- 0. Index setup for better join/delete performance
    -- ==========================================
    CREATE INDEX IF NOT EXISTS idx_flight_points_flightid ON flight_points(flightid);

    -- ==========================================
    -- 1. Compute number of points per trajectory
    -- ==========================================
    DROP TABLE IF EXISTS trip_sizes;
    CREATE TEMP TABLE trip_sizes AS
    SELECT flightid AS trip_id, COUNT(*) AS n_points
    FROM flight_points
    GROUP BY flightid;

    CREATE INDEX ON trip_sizes(trip_id);

    -- ==========================================
    -- 2. Randomly sample trajectories up to target size
    -- (fast approximate method — avoids full-table shuffle)
    -- ==========================================
    DROP TABLE IF EXISTS sampled_trajectories;
    CREATE TEMP TABLE sampled_trajectories AS
    SELECT trip_id
    FROM trip_sizes
    ORDER BY random()
    LIMIT (
        SELECT CEIL($TARGET_POINTS::numeric / NULLIF((SELECT AVG(n_points) FROM trip_sizes), 0))
    );

    ANALYZE sampled_trajectories;

    -- ==========================================
    -- 3. Delete all points not in sampled trajectories (fast anti-join)
    -- ==========================================
    DELETE FROM flight_points fp
    WHERE NOT EXISTS (
        SELECT 1
        FROM sampled_trajectories st
        WHERE st.trip_id = fp.flightid
    );

    -- ==========================================
    -- 4. Refresh statistics
    -- ==========================================
    ANALYZE flight_points;
    
    CREATE INDEX IF NOT EXISTS idx_flight_points_flight_id_ts ON flight_points(flightid, timestamp);
    -- Parameterized chunk size for easy tweaking
    WITH params AS (
    SELECT 5000::int AS chunk_size
    ),

    -- Step 1: Identify flights with too many points
    large_flights AS (
    SELECT flightid
    FROM flight_points, params
    GROUP BY flightid
    HAVING COUNT(*) > (SELECT chunk_size FROM params)
    ),

    -- Step 2: Assign a row number within each large flight
    numbered AS (
    SELECT
        fp.ctid,
        fp.flightid,
        ROW_NUMBER() OVER (PARTITION BY fp.flightid ORDER BY fp.timestamp) AS rn,
        p.chunk_size
    FROM flight_points fp
    JOIN large_flights lf ON fp.flightid = lf.flightid
    JOIN params p ON TRUE
    ),

    -- Step 3: Compute a unique new flightid for each chunk
    to_update AS (
    SELECT
        n.ctid,
        n.flightid * 10000 + FLOOR((n.rn - 1) / n.chunk_size)::int AS new_flightid
    FROM numbered n
    )

    -- Step 4: Apply the update safely
    UPDATE flight_points AS fp
    SET flightid = u.new_flightid
    FROM to_update AS u
    WHERE fp.ctid = u.ctid;



    ALTER TABLE flight_points ALTER COLUMN geom TYPE Geometry(Point, 3857) USING ST_Transform(geom, 3857);
    ALTER TABLE counties ALTER COLUMN geom TYPE Geometry(Polygon, 3857) USING ST_Transform(geom, 3857);
    ALTER TABLE districts ALTER COLUMN geom TYPE Geometry(Polygon, 3857) USING ST_Transform(geom, 3857);
    ALTER TABLE municipalities ALTER COLUMN geom TYPE Geometry(Polygon, 3857) USING ST_Transform(geom, 3857);

EOF
fi
        #delete trips with only 1 entry 
        # Now deduplicate and build trajectories in one go
        psql -U "$DB_USER" -d "$DB_NAME" -h "$DB_HOST" <<EOF

        WITH large_flights AS (
            SELECT flightid
            FROM flight_points
            GROUP BY flightid
            HAVING COUNT(*) > 5000  -- only split flights with > 2500
        ),
        numbered AS (
            SELECT
                fp.ctid,  -- must qualify with alias
                fp.flightid,
                FLOOR((ROW_NUMBER() OVER (PARTITION BY fp.flightid ORDER BY fp.timestamp) - 1) / 5000)::int AS chunk_id
            FROM flight_points fp
            JOIN large_flights lf ON fp.flightid = lf.flightid
        )
        UPDATE flight_points AS fp
        SET flightid = n.flightid * 1000 + n.chunk_id
        FROM numbered AS n
        WHERE fp.ctid = n.ctid;


        WITH deduplicated_points AS (
        SELECT DISTINCT ON (flightid, timestamp)
            flightid, airplanetype, origin, destination, altitude, timestamp, geom
        FROM flight_points
        ORDER BY flightid, timestamp
        ),
        ordered_points AS (
        SELECT *,
                LAG(timestamp) OVER (PARTITION BY flightid ORDER BY timestamp) AS prev_ts
        FROM deduplicated_points
        ),
        clean_points AS (
        SELECT * FROM ordered_points
        WHERE prev_ts IS NULL OR timestamp > prev_ts
        )
        -- Now build sequences using clean_points
        INSERT INTO flights (flightid, airplanetype, origin, destination, altitude, trip)
        SELECT 
            flightid,
            MIN(airplanetype) AS airplanetype,
            MIN(origin) AS origin,
            MIN(destination) AS destination,
            tfloatSeq(
                array_agg(tfloat(altitude, timestamp AT TIME ZONE 'UTC') ORDER BY timestamp) 
                FILTER (WHERE altitude IS NOT NULL)
            ),
            tgeompointseq(
                array_agg(tgeompoint(geom, timestamp AT TIME ZONE 'UTC') ORDER BY timestamp),
                'discrete'
            )
        FROM clean_points
        GROUP BY flightid;

 

        -- time partitioning
        DROP TABLE IF EXISTS flights_AdaptTimeGrid;
        DROP TABLE IF EXISTS AdaptiveBins;

        CREATE TABLE AdaptiveBins(BinId, Period) AS
        WITH Times(T) AS (
        SELECT unnest(timestamps(trip))
        FROM flights
        ORDER BY 1 ),
        MaxTime(MaxT) AS (
        SELECT MAX(T) FROM Times ),
        Bins1(BinId, T) AS (
        SELECT NTILE(4) OVER(ORDER BY T), T
        FROM Times ),
        Bins2(BinId, T, RowNo) AS (
        SELECT BinId, T, ROW_NUMBER() OVER (PARTITION BY BinId ORDER BY T)
        FROM Bins1 )
        SELECT BinId, span(T, COALESCE(LEAD(T, 1) OVER (ORDER BY T), MaxT))
        FROM Bins2, MaxTime
        WHERE RowNo = 1;

        CREATE TABLE flights_AdaptTimeGrid(LIKE flights, TileId text)
        PARTITION BY LIST(TileId);
        CREATE TABLE flights_AdaptTimeGrid_1 PARTITION OF flights_AdaptTimeGrid FOR VALUES IN (1);
        CREATE TABLE flights_AdaptTimeGrid_2 PARTITION OF flights_AdaptTimeGrid FOR VALUES IN (2);
        CREATE TABLE flights_AdaptTimeGrid_3 PARTITION OF flights_AdaptTimeGrid FOR VALUES IN (3);
        CREATE TABLE flights_AdaptTimeGrid_4 PARTITION OF flights_AdaptTimeGrid FOR VALUES IN (4);

        INSERT INTO flights_AdaptTimeGrid(flightid, airplanetype, origin, destination, altitude, trip, TileId)
        SELECT c.flightid, c.airplanetype, c.origin, c.destination, c.altitude, c.trip, ab.BinId
        FROM flights c, AdaptiveBins ab
        WHERE atTime(c.trip, Period) IS NOT NULL;


        DROP TABLE IF EXISTS allpoints;
        DROP TABLE IF EXISTS XBins;
        DROP TABLE IF EXISTS YBins;
        DROP TABLE IF EXISTS AdaptiveGrid;
        DROP TABLE IF EXISTS flights_AdaptGrid;
        

        CREATE TABLE AllPoints(Point) AS
        SELECT getValue(unnest(instants(trip)))
        FROM flights;


        CREATE TABLE XBins(XBinId, SpanX) AS
        WITH Bins1(BinId, X) AS (
        SELECT NTILE(2) OVER(ORDER BY ST_X(Point)), ST_X(Point)
        FROM AllPoints ),
        Bins2(BinId, X, RowNo) AS (
        SELECT BinId, X, ROW_NUMBER() OVER (PARTITION BY BinId ORDER BY X)
        FROM Bins1 )
        SELECT BinId, span(X, COALESCE(LEAD(X, 1) OVER (ORDER BY X),
        (SELECT MAX(ST_X(Point)) FROM AllPoints)))
        FROM Bins2
        WHERE RowNo = 1;

        CREATE TABLE YBins(YBinId, SpanY) AS
        WITH Bins1(BinId, Y) AS (
        SELECT NTILE(2) OVER(ORDER BY ST_Y(Point)), ST_Y(Point)
        FROM AllPoints ),
        Bins2(BinId, Y, RowNo) AS (
        SELECT BinId, Y, ROW_NUMBER() OVER (PARTITION BY BinId ORDER BY Y)
        FROM Bins1 )
        SELECT BinId, span(Y, COALESCE(LEAD(Y, 1) OVER (ORDER BY Y),
        (SELECT MAX(ST_Y(Point)) FROM AllPoints)))
        FROM Bins2
        WHERE RowNo = 1;

        CREATE TABLE AdaptiveGrid(TileId, RowNo, ColNo, Tile, Geom) AS
        WITH TableSRID(SRID) AS (
        SELECT ST_SRID(Point) FROM AllPoints LIMIT 1 ),
        Tiles(TileId, RowNo, ColNo, Tile) AS (
        SELECT ROW_NUMBER() OVER(), YBinId, XBinId,
        stboxX(lower(SpanX), lower(SpanY), upper(SpanX), upper(SpanY), SRID)
        FROM YBins y, XBins x, TableSRID s
        ORDER BY YBinId, XBinId )
        SELECT TileId, RowNo, ColNo, Tile, Tile::geometry
        FROM Tiles;

        CREATE TABLE flights_AdaptGrid(LIKE flights, TileId text)
        PARTITION BY LIST(TileId);

        CREATE TABLE flights_AdaptGrid_1 PARTITION OF flights_AdaptGrid
        FOR VALUES IN (1);
        CREATE TABLE flights_AdaptGrid_2 PARTITION OF flights_AdaptGrid
        FOR VALUES IN (2);
        CREATE TABLE flights_AdaptGrid_3 PARTITION OF flights_AdaptGrid
        FOR VALUES IN (3);
        CREATE TABLE flights_AdaptGrid_4 PARTITION OF flights_AdaptGrid
        FOR VALUES IN (4);

        INSERT INTO flights_AdaptGrid(flightid, airplanetype, origin, destination, altitude, trip, TileId)
        SELECT c.flightid, c.airplanetype, c.origin, c.destination, c.altitude, c.trip, ag.TileId
        FROM flights c, AdaptiveGrid ag
        WHERE atStbox(c.trip, ag.Tile, false) IS NOT NULL;




        CREATE TABLE IF NOT EXISTS tsdb_flight_points AS
        SELECT 
            flightid,
            airplanetype,
            origin,
            destination,
            geom,
            timestamp,
            altitude
        FROM flight_points;



        Select create_hypertable('tsdb_flight_points', by_range('timestamp'));

        CREATE TABLE IF NOT EXISTS flight_trajectories AS
        SELECT 
            flightid,
            airplanetype,
            origin,
            destination,
            startValue(trip) AS origin_geom,
            endValue(trip) AS destination_geom,
            startTimestamp(trip) AS start_time,
            endTimestamp(trip) AS end_time
        FROM flights;

        CREATE TABLE IF NOT EXISTS tsdb_flight_trajectories AS 
        SELECT 
            flightid,
            airplanetype,
            origin,
            destination,
            origin_geom,
            destination_geom,
            start_time,
            end_time
        FROM flight_trajectories;


        SELECT create_hypertable('tsdb_flight_trajectories', 'start_time', chunk_time_interval => interval '1 day', migrate_data => TRUE);

EOF
        psql -U "$DB_USER" -d "$DB_NAME" -h "$DB_HOST" <<EOF
        CREATE INDEX IF NOT EXISTS idx_cities_geom ON cities USING gist (geom);
        CREATE INDEX IF NOT EXISTS idx_counties_geom ON counties USING gist (geom);
        CREATE INDEX IF NOT EXISTS idx_districts_geom ON districts USING gist (geom);
        CREATE INDEX IF NOT EXISTS idx_municipalities_geom ON municipalities USING gist (geom);
        CREATE INDEX IF NOT EXISTS idx_flight_points_geom ON flight_points USING gist (geom);
        CREATE INDEX IF NOT EXISTS idx_flight_points_timestamp ON flight_points (timestamp);
        
        CREATE INDEX ON flight_trajectories USING GIST (origin_geom);
        CREATE INDEX ON flight_trajectories USING GIST (destination_geom);
        CREATE INDEX ON flight_trajectories(start_time);
        CREATE INDEX ON flight_trajectories(end_time);

        CREATE INDEX ON tsdb_flight_trajectories USING GIST (origin_geom);
        CREATE INDEX ON tsdb_flight_trajectories USING GIST (destination_geom);
        CREATE INDEX ON tsdb_flight_trajectories(start_time);
        CREATE INDEX ON tsdb_flight_trajectories(end_time);

        CREATE INDEX IF NOT EXISTS idx_flights_trip ON flights USING gist (trip);
        
        CREATE INDEX IF NOT EXISTS idx_tsdb_flight_points_geom ON tsdb_flight_points USING gist (geom);
        CREATE INDEX IF NOT EXISTS idx_tsdb_flight_points_timestamp ON tsdb_flight_points (timestamp);
        
        --- time partitioning indexes
        CREATE INDEX IF NOT EXISTS idx_flights_adapttimegrid_geom ON flights_AdaptTimeGrid USING gist (trip);
        CREATE INDEX IF NOT EXISTS idx_flights_adapttimegrid_1_geom ON flights_AdaptTimeGrid_1 USING gist (trip);
        CREATE INDEX IF NOT EXISTS idx_flights_adapttimegrid_2_geom ON flights_AdaptTimeGrid_2 USING gist (trip);
        CREATE INDEX IF NOT EXISTS idx_flights_adapttimegrid_3_geom ON flights_AdaptTimeGrid_3 USING gist (trip);
        CREATE INDEX IF NOT EXISTS idx_flights_adapttimegrid_4_geom ON flights_AdaptTimeGrid_4 USING gist (trip);


        -- create on partitions
        CREATE INDEX IF NOT EXISTS idx_flights_adaptgrid_geom ON flights_AdaptGrid USING gist (trip);
        CREATE INDEX IF NOT EXISTS idx_flights_adaptgrid_1_geom ON flights_AdaptGrid_1 USING gist (trip);
        CREATE INDEX IF NOT EXISTS idx_flights_adaptgrid_2_geom ON flights_AdaptGrid_2 USING gist (trip);
        CREATE INDEX IF NOT EXISTS idx_flights_adaptgrid_3_geom ON flights_AdaptGrid_3 USING gist (trip);
        CREATE INDEX IF NOT EXISTS idx_flights_adaptgrid_4_geom ON flights_AdaptGrid_4 USING gist (trip);


EOF
    elif [ "$application" == "cycling" ]; then
        psql -U "$DB_USER" -d "$DB_NAME" -h "$DB_HOST" <<EOF
        -- Enable required extensions
        CREATE EXTENSION IF NOT EXISTS postgis;
        CREATE EXTENSION IF NOT EXISTS mobilitydb;
        CREATE EXTENSION IF NOT EXISTS timescaledb;
        DROP TABLE IF EXISTS ride_points;
        DROP TABLE IF EXISTS ride_trajectories;
        DROP TABLE IF EXISTS rides;
        DROP TABLE IF EXISTS tsdb_ride_points;
        DROP TABLE IF EXISTS tsdb_ride_trajectories;
        DROP TABLE IF EXISTS berlin_districts;
        DROP TABLE IF EXISTS universities;
        CREATE TABLE ride_points (
            ride_id integer, 
            rider_id integer, 
            geom Geometry(Point, 4326),
            timestamp timestamp
        );
        CREATE TABLE rides (
            ride_id integer,
            rider_id integer, 
            trip tgeompoint
        );

        CREATE TABLE berlin_districts (
            name TEXT,
            geom Geometry(Polygon, 4326)
        );
        CREATE TABLE universities (
            name TEXT,
            geom Geometry(MultiPolygon, 4326)
        );
    

        -- load districts and universities from /home/tim/data/cycling/resources, files named berlin-districts-wkt-simplified.csv and universities-wkt-simplified.csv
        CREATE TABLE IF NOT EXISTS temp_districts_raw (
            name TEXT,
            wkt TEXT
        );
        \copy temp_districts_raw(name, wkt) FROM '/home/tim/data/cycling/resources/berlin-districts-wkt-simplified.csv' WITH (FORMAT csv, DELIMITER ';', HEADER true)
        -- Insert parsed WKT into final table
        INSERT INTO berlin_districts (name, geom)
        SELECT name, 
        ST_GeomFromText(wkt, 4326) 
        FROM temp_districts_raw;    
        DROP TABLE IF EXISTS temp_districts_raw;

        CREATE TABLE IF NOT EXISTS temp_universities_raw (
            name TEXT,
            wkt TEXT
        );
        \copy temp_universities_raw(name, wkt) FROM '/home/tim/data/cycling/resources/universities-wkt-simplified.csv' WITH (FORMAT csv, DELIMITER ';', HEADER true);
        INSERT INTO universities (name, geom)
        SELECT name, 
        ST_GeomFromText(wkt, 4326)
        FROM temp_universities_raw;
        DROP TABLE IF EXISTS temp_universities_raw;
        
EOF
        for file in /home/tim/data/cycling/merged*.csv; do
            echo "Processing $file..."
            psql -U "$DB_USER" -d "$DB_NAME" -h "$DB_HOST" <<EOF
            -- Use a temp table to parse and cast the input
            DROP TABLE IF EXISTS temp_ride_raw;
            CREATE TEMP TABLE temp_ride_raw (
                ride_id integer,
                rider_id integer,
                longitude TEXT,
                latitude TEXT,
                timestamp TIMESTAMP
            );
            \copy temp_ride_raw FROM '$file' WITH (FORMAT csv, DELIMITER ',', HEADER true)
            INSERT INTO ride_points (ride_id, rider_id, geom, timestamp)
            SELECT
                ride_id,
                rider_id,
                
        ('POINT(' || latitude || ' ' || longitude || ')'),
                timestamp
            FROM temp_ride_raw;
            DROP TABLE IF EXISTS temp_ride_raw;

EOF
        done
    #delete all single row trips
    psql -U "$DB_USER" -d "$DB_NAME" -h "$DB_HOST" <<EOF
    DELETE FROM ride_points
    WHERE ride_id IN (
        SELECT ride_id
        FROM ride_points
        GROUP BY ride_id
        HAVING COUNT(*) = 1
    );
EOF
#delete all values where timestamp is below 2018-01-01
    psql -U "$DB_USER" -d "$DB_NAME" -h "$DB_HOST" <<EOF
    DELETE FROM ride_points
    WHERE timestamp < '2020-01-01';
EOF
        # --- Subset Cycling trajectories if requested ---
if [ "$TARGET_POINTS" -gt 0 ]; then
    echo "Subsetting Cycling dataset to approximately $size ($TARGET_POINTS points)..."
    psql -U "$DB_USER" -d "$DB_NAME" -h "$DB_HOST" <<EOF
    DROP TABLE IF EXISTS trip_sizes;
    CREATE TEMP TABLE trip_sizes AS
    SELECT ride_id AS trip_id, COUNT(*) AS n_points
    FROM ride_points
    GROUP BY ride_id;

    WITH cumulative AS (
        SELECT trip_id, n_points,
               SUM(n_points) OVER (ORDER BY random()) AS cum_sum
        FROM trip_sizes
    )
    SELECT trip_id INTO TEMP TABLE sampled_trajectories
    FROM cumulative
    WHERE cum_sum <= $TARGET_POINTS;

    DELETE FROM ride_points
    WHERE ride_id NOT IN (SELECT trip_id FROM sampled_trajectories);

    ANALYZE ride_points;

    WITH ranked AS (
        SELECT
            ctid,
            ROW_NUMBER() OVER (PARTITION BY ride_id, timestamp ORDER BY rider_id) AS rn
        FROM ride_points
    )
    DELETE FROM ride_points
    WHERE ctid IN (
        SELECT ctid
        FROM ranked
        WHERE rn > 1
    );

    ANALYZE ride_points;

    ALTER TABLE ride_points ALTER COLUMN geom TYPE Geometry(Point, 3857) USING ST_Transform(geom, 3857);
    ALTER TABLE berlin_districts ALTER COLUMN geom TYPE Geometry(Polygon, 3857) USING ST_Transform(geom, 3857);
    ALTER TABLE universities ALTER COLUMN geom TYPE Geometry(MultiPolygon, 3857) USING ST_Transform(geom, 3857);

EOF
fi

        # Now deduplicate and build trajectories in one go
        psql -U "$DB_USER" -d "$DB_NAME" -h "$DB_HOST" <<EOF
        
        -- split large rides
        WITH large_rides AS (
            SELECT ride_id
            FROM ride_points
            GROUP BY ride_id
            HAVING COUNT(*) > 5000  -- only split rides with > 5000 points
        ),

        numbered AS (
            SELECT
                rp.ctid,  -- must qualify with alias
                rp.ride_id,
                FLOOR((ROW_NUMBER() OVER (PARTITION BY rp.ride_id ORDER BY rp.timestamp) - 1) / 5000)::int AS chunk_id
            FROM ride_points rp
            JOIN large_rides lr ON rp.ride_id = lr.ride_id
        )
        UPDATE ride_points AS rp
        SET ride_id = n.ride_id * 1000 + n.chunk_id
        FROM numbered AS n
        WHERE rp.ctid = n.ctid;


        WITH deduplicated_points AS (
            SELECT DISTINCT ride_id, rider_id, geom, timestamp
            FROM ride_points
        ),
        ordered_points AS (
            SELECT *
            FROM deduplicated_points
            ORDER BY ride_id, timestamp
        )
        INSERT INTO rides (ride_id, rider_id, trip)
        SELECT 
            ride_id,
            MIN(rider_id) AS rider_id,  -- keep one rider_id per ride (if multiple, pick any)
            tgeompointseq(
                array_agg(tgeompoint(geom, timestamp AT TIME ZONE 'UTC') ORDER BY timestamp),
                'discrete'
            )
        FROM ordered_points
        GROUP BY ride_id;

        -- time partitioning
        DROP TABLE IF EXISTS AdaptiveBins;
        DROP TABLE IF EXISTS rides_AdaptTimeGrid;
        

        CREATE TABLE AdaptiveBins(BinId, Period) AS
        WITH Times(T) AS (
        SELECT unnest(timestamps(trip))
        FROM rides
        ORDER BY 1 ),
        MaxTime(MaxT) AS (
        SELECT MAX(T) FROM Times ),
        Bins1(BinId, T) AS (
        SELECT NTILE(4) OVER(ORDER BY T), T
        FROM Times ),
        Bins2(BinId, T, RowNo) AS (
        SELECT BinId, T, ROW_NUMBER() OVER (PARTITION BY BinId ORDER BY T)
        FROM Bins1 )
        SELECT BinId, span(T, COALESCE(LEAD(T, 1) OVER (ORDER BY T), MaxT))
        FROM Bins2, MaxTime
        WHERE RowNo = 1;

        CREATE TABLE rides_AdaptTimeGrid(LIKE rides, BinId integer)
        PARTITION BY LIST(BinId);
        CREATE TABLE rides_AdaptTimeGrid_1 PARTITION OF rides_AdaptTimeGrid
        FOR VALUES IN (1);
        CREATE TABLE rides_AdaptTimeGrid_2 PARTITION OF rides_AdaptTimeGrid
        FOR VALUES IN (2);
        CREATE TABLE rides_AdaptTimeGrid_3 PARTITION OF rides_AdaptTimeGrid
        FOR VALUES IN (3);
        CREATE TABLE rides_AdaptTimeGrid_4 PARTITION OF rides_AdaptTimeGrid
        FOR VALUES IN (4);

        INSERT INTO rides_AdaptTimeGrid(ride_id, rider_id, trip, BinId)
        SELECT c.ride_id, c.rider_id, c.trip, ab.BinId
        FROM rides c, AdaptiveBins ab
        WHERE atTime(Trip, Period) IS NOT NULL;



        -- space partitioning
        DROP TABLE IF EXISTS allpoints;
        DROP TABLE IF EXISTS XBins;
        DROP TABLE IF EXISTS YBins;
        DROP TABLE IF EXISTS AdaptiveGrid;
        DROP TABLE IF EXISTS rides_AdaptGrid;


        CREATE TABLE AllPoints(Point) AS
        SELECT getValue(unnest(instants(trip)))
        FROM rides;


        CREATE TABLE XBins(XBinId, SpanX) AS
        WITH Bins1(BinId, X) AS (
        SELECT NTILE(2) OVER(ORDER BY ST_X(Point)), ST_X(Point)
        FROM AllPoints ),
        Bins2(BinId, X, RowNo) AS (
        SELECT BinId, X, ROW_NUMBER() OVER (PARTITION BY BinId ORDER BY X)
        FROM Bins1 )
        SELECT BinId, span(X, COALESCE(LEAD(X, 1) OVER (ORDER BY X),
        (SELECT MAX(ST_X(Point)) FROM AllPoints)))
        FROM Bins2
        WHERE RowNo = 1;

        CREATE TABLE YBins(YBinId, SpanY) AS
        WITH Bins1(BinId, Y) AS (
        SELECT NTILE(2) OVER(ORDER BY ST_Y(Point)), ST_Y(Point)
        FROM AllPoints ),
        Bins2(BinId, Y, RowNo) AS (
        SELECT BinId, Y, ROW_NUMBER() OVER (PARTITION BY BinId ORDER BY Y)
        FROM Bins1 )
        SELECT BinId, span(Y, COALESCE(LEAD(Y, 1) OVER (ORDER BY Y),
        (SELECT MAX(ST_Y(Point)) FROM AllPoints)))
        FROM Bins2
        WHERE RowNo = 1;

        CREATE TABLE AdaptiveGrid(TileId, RowNo, ColNo, Tile, Geom) AS
        WITH TableSRID(SRID) AS (
        SELECT ST_SRID(Point) FROM AllPoints LIMIT 1 ),
        Tiles(TileId, RowNo, ColNo, Tile) AS (
        SELECT ROW_NUMBER() OVER(), YBinId, XBinId,
        stboxX(lower(SpanX), lower(SpanY), upper(SpanX), upper(SpanY), SRID)
        FROM YBins y, XBins x, TableSRID s
        ORDER BY YBinId, XBinId )
        SELECT TileId, RowNo, ColNo, Tile, Tile::geometry
        FROM Tiles;

        CREATE TABLE rides_AdaptGrid(LIKE rides, TileId text)
        PARTITION BY LIST(TileId);

        CREATE TABLE rides_AdaptGrid_1 PARTITION OF rides_AdaptGrid
        FOR VALUES IN (1);
        CREATE TABLE rides_AdaptGrid_2 PARTITION OF rides_AdaptGrid
        FOR VALUES IN (2);
        CREATE TABLE rides_AdaptGrid_3 PARTITION OF rides_AdaptGrid
        FOR VALUES IN (3);
        CREATE TABLE rides_AdaptGrid_4 PARTITION OF rides_AdaptGrid
        FOR VALUES IN (4);

        INSERT INTO rides_AdaptGrid(ride_id, rider_id, trip, TileId)
        SELECT c.ride_id, c.rider_id, c.trip, ag.TileId
        FROM rides c, AdaptiveGrid ag
        WHERE atStbox(c.trip, ag.Tile, false) IS NOT NULL;

        CREATE TABLE IF NOT EXISTS tsdb_ride_points AS
        SELECT
            ride_id,
            rider_id,
            geom,
            timestamp
        FROM ride_points;
        SELECT create_hypertable('tsdb_ride_points', 'timestamp', if_not_exists => TRUE, migrate_data => TRUE);


        -- Ride trajectories (PostGIS)
        CREATE TABLE ride_trajectories AS
        SELECT
            ride_id,
            rider_id,
            startValue(trip) as origin_geom,
            endValue(trip) as destination_geom,
            startTimestamp(trip) as start_time,
            endTimestamp(trip) as end_time
        FROM rides;



        -- Ride trajectories (TimescaleDB)
        CREATE TABLE tsdb_ride_trajectories AS
        SELECT
            ride_id,
            rider_id,
            origin_geom,
            destination_geom,
            start_time,
            end_time
        FROM ride_trajectories;

        SELECT create_hypertable('tsdb_ride_trajectories', 'start_time', if_not_exists => TRUE, migrate_data => TRUE);


EOF
        psql -U "$DB_USER" -d "$DB_NAME" -h "$DB_HOST" <<EOF
        CREATE INDEX IF NOT EXISTS idx_districts_geom ON districts USING gist (geom);
        CREATE INDEX IF NOT EXISTS idx_universities_geom ON universities USING gist (geom);
        CREATE INDEX IF NOT EXISTS idx_ride_points_geom ON ride_points USING gist (geom);
        CREATE INDEX IF NOT EXISTS idx_ride_points_timestamp ON ride_points (timestamp);       
        CREATE INDEX IF NOT EXISTS idx_cycling_trips_trip ON rides USING gist (trip);
        CREATE INDEX IF NOT EXISTS idx_tsdb_cycling_points_geom ON tsdb_ride_points USING gist (geom);
        CREATE INDEX IF NOT EXISTS idx_tsdb_cycling_points_timestamp ON tsdb_ride_points (timestamp);

        CREATE INDEX ON ride_trajectories USING GIST (origin_geom);
        CREATE INDEX ON ride_trajectories USING GIST (destination_geom);
        CREATE INDEX ON ride_trajectories (start_time);
        CREATE INDEX ON ride_trajectories (end_time);


        CREATE INDEX ON tsdb_ride_trajectories USING GIST (origin_geom);
        CREATE INDEX ON tsdb_ride_trajectories USING GIST (destination_geom);
        CREATE INDEX ON tsdb_ride_trajectories (start_time);
        CREATE INDEX ON tsdb_ride_trajectories (end_time);


        -- create on time partitions
        CREATE INDEX IF NOT EXISTS idx_rides_adapttimegrid_geom ON rides_AdaptTimeGrid USING gist (trip);
        CREATE INDEX IF NOT EXISTS idx_rides_adapttimegrid_1_geom ON rides_AdaptTimeGrid_1 USING gist (trip);
        CREATE INDEX IF NOT EXISTS idx_rides_adapttimegrid_2_geom ON rides_AdaptTimeGrid_2 USING gist (trip);
        CREATE INDEX IF NOT EXISTS idx_rides_adapttimegrid_3_geom ON rides_AdaptTimeGrid_3 USING gist (trip);
        CREATE INDEX IF NOT EXISTS idx_rides_adapttimegrid_4_geom ON rides_AdaptTimeGrid_4 USING gist (trip);


        -- create on space partitions
        CREATE INDEX IF NOT EXISTS idx_rides_adaptgrid_geom ON rides_AdaptGrid USING gist (trip);
        CREATE INDEX IF NOT EXISTS idx_rides_adaptgrid_1_geom ON rides_AdaptGrid_1 USING gist (trip);
        CREATE INDEX IF NOT EXISTS idx_rides_adaptgrid_2_geom ON rides_AdaptGrid_2 USING gist (trip);
        CREATE INDEX IF NOT EXISTS idx_rides_adaptgrid_3_geom ON rides_AdaptGrid_3 USING gist (trip);
        CREATE INDEX IF NOT EXISTS idx_rides_adaptgrid_4_geom ON rides_AdaptGrid_4 USING gist (trip);

EOF
fi
fi