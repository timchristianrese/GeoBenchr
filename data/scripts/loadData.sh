#!/bin/bash
system=$1
application=$2
if [ "$system" == "mobilitydb" ]; then
    echo "Loading data into mobilitydb"
# Set PostgreSQL connection parameters
    DB_NAME="postgres"
    DB_USER="postgres"
    DB_HOST="localhost"
    DB_PASSWORD="test"
    export PGPASSWORD=$DB_PASSWORD
    if [ "$application" == "ais" ]; then
        psql -U "$DB_USER" -d "$DB_NAME" -h "$DB_HOST" <<EOF
        -- Enable required extensions
        CREATE EXTENSION IF NOT EXISTS postgis;
        CREATE EXTENSION IF NOT EXISTS mobilitydb;
        CREATE EXTENSION IF NOT EXISTS timescaledb;
        DROP TABLE IF EXISTS crossings;
        DROP TABLE IF EXISTS crossing_points;
        DROP TABLE IF EXISTS tsdb_crossing_points;
        CREATE TABLE crossings (
            crossing_id integer, 
            traj tgeogpoint
        );
        CREATE TABLE crossing_points (
            crossing_id integer,
            timestamp timestamp,
            vessel_id Text,
            geom Geography(Point, 4326),
            heading float,
            speed float,
            course float
        );
EOF
# Insert the points
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
        ST_GeogFromText(geom_txt),
        heading::float,
        speed::float,
        course::float
    FROM temp_crossing_raw;
EOF
done

# Now deduplicate and build trajectories in one go
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
INSERT INTO crossings (crossing_id, traj)
SELECT 
    crossing_id,
    tgeogpointseq(
        array_agg(tgeogpoint(geom, timestamp) ORDER BY timestamp),
        'linear'
    )
FROM deduplicated_points
GROUP BY crossing_id;

CREATE TABLE IF NOT EXISTS tsdb_crossing_points (
    crossing_id integer,
    timestamp timestamp,
    vessel_id Text,
    geom Geography(Point, 4326),
    heading float,
    speed float,
    course float
);
INSERT INTO tsdb_crossing_points (crossing_id, timestamp, vessel_id, geom, heading, speed, course)
SELECT crossing_id, timestamp, vessel_id, geom, heading, speed, course FROM crossing_points;
SELECT create_hypertable('tsdb_crossing_points', 'timestamp', chunk_time_interval => interval '1 day', if_not_exists => true);
EOF

    psql -U "$DB_USER" -d "$DB_NAME" -h "$DB_HOST" <<EOF
    CREATE INDEX IF NOT EXISTS idx_crossing_points_geom ON crossing_points USING gist (geom);
    CREATE INDEX IF NOT EXISTS ON crossing_points (timestamp);

    CREATE INDEX IF NOT EXISTS idx_crossings_geom ON crossings USING gist (traj);

    CREATE INDEX IF NOT EXISTS idx_tsdb_crossing_points_geom ON tsdb_crossing_points USING gist (geom);
    CREATE INDEX IF NOT EXISTS idx_tsdb_crossing_points_timestamp ON tsdb_crossing_points (timestamp);
EOF
    elif [ "$application" == "aviation" ]; then
        psql -U "$DB_USER" -d "$DB_NAME" -h "$DB_HOST" <<EOF
        -- Enable required extensions
        CREATE EXTENSION IF NOT EXISTS postgis;
        CREATE EXTENSION IF NOT EXISTS mobilitydb;
        CREATE EXTENSION IF NOT EXISTS timescaledb;
        DROP TABLE IF EXISTS flight_points;
        DROP TABLE IF EXISTS flights;
        DROP TABLE IF EXISTS tsdb_flight_points;
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
            geom           GEOGRAPHY(Point, 4326),
            timestamp            TIMESTAMP,
            altitude       FLOAT
        );

        CREATE TABLE IF NOT EXISTS flights (
            flightid BIGINT,
            airplanetype TEXT,
            origin TEXT,
            destination TEXT,
            altitude tfloat,
            trip tgeogpoint
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
            geom        GEOGRAPHY(Point, 4326) GENERATED ALWAYS AS (
                ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)
            ) STORED
        );
        CREATE TABLE counties (
            name     TEXT,
            geom  GEOGRAPHY(Polygon, 4326)
        );

        CREATE TABLE districts (
            name     TEXT,
            geom  GEOGRAPHY(Polygon, 4326)
        );

        CREATE TABLE municipalities (
            name     TEXT,
            geom  GEOGRAPHY(Polygon, 4326)
        );
EOF
        psql -U "$DB_USER" -d "$DB_NAME" -h "$DB_HOST" <<EOF

        \copy cities FROM '/home/tim/data/aviation/resources/cities.csv' WITH (FORMAT csv, HEADER true, DELIMITER ';');
EOF
        

        for table in counties districts municipalities; do
            echo "Loading $table..."
            psql -U "$DB_USER" -d "$DB_NAME" -h "$DB_HOST" <<EOF
        CREATE TABLE IF NOT EXISTS temp_${table}_raw (
            name TEXT,
            wkt TEXT
        );
        \copy temp_${table}_raw(name, wkt) FROM '/home/tim/data/aviation/resources/${table}-wkt.csv' WITH (FORMAT csv, DELIMITER ';', HEADER true)

        -- Insert parsed WKT into final table
        INSERT INTO ${table} (name, geom)
        SELECT name, ST_GeomFromText(wkt, 4326) FROM temp_${table}_raw;

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
        # Now deduplicate and build trajectories in one go
        psql -U "$DB_USER" -d "$DB_NAME" -h "$DB_HOST" <<EOF
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
                FILTER (WHERE altitude IS NOT NULL),
                'step'
            ),
            tgeogpointseq(
                array_agg(tgeogpoint(geom, timestamp AT TIME ZONE 'UTC') ORDER BY timestamp),
                'linear'
            )
        FROM clean_points
        GROUP BY flightid;


        CREATE TABLE IF NOT EXISTS tsdb_flight_points (
            flightid BIGINT,
            airplanetype TEXT,
            origin TEXT,
            destination TEXT,
            geom GEOGRAPHY(Point, 4326),
            timestamp TIMESTAMPTZ,
            altitude FLOAT
        );
        Select create_hypertable('tsdb_flight_points', by_range('timestamp'));
        INSERT INTO tsdb_flight_points (flightid, airplanetype, origin, destination, geom, timestamp, altitude)
        SELECT flightid, airplanetype, origin, destination, geom, timestamp, altitude FROM flight_points;
EOF
        psql -U "$DB_USER" -d "$DB_NAME" -h "$DB_HOST" <<EOF
        CREATE INDEX IF NOT EXISTS idx_flight_points_geom ON flight_points USING gist (geom);
        CREATE INDEX IF NOT EXISTS idx_flight_points_timestamp ON flight_points (timestamp);

        CREATE INDEX IF NOT EXISTS idx_flights_trip ON flights USING gist (trip);
        
        CREATE INDEX IF NOT EXISTS idx_tsdb_flight_points_geom ON tsdb_flight_points USING gist (geom);
        CREATE INDEX IF NOT EXISTS idx_tsdb_flight_points_timestamp ON tsdb_flight_points (timestamp);
EOF
    fi
fi