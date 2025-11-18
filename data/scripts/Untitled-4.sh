#!/bin/bash


SPLIT_DIR="/home/tim/data/geomesa/split_flight_points"

# GeoMesa ingest settings
GEOMESA_BIN="/opt/geomesa-accumulo/bin/geomesa-accumulo"
GEOMESA_INGEST_JAVA_OPTS="-Xmx1G -Xms512M"   # adjust as needed
ACCUMULO_INSTANCE="test"
ZOOKEEPERS="server-peter-lan"
USER="root"
PASSWORD="test"
CATALOG="example"
SPEC="$flight_points_spec"
FEATURE_NAME="flight_points"
CONF="/opt/geomesa-accumulo/flight_points.conf"
THREADS=1

export GEOMESA_INGEST_JAVA_OPTS

# Loop over all split files and ingest
for split_file in "$SPLIT_DIR"/flight_points*_part_*; do
    echo "Ingesting split file: $split_file"
    "$GEOMESA_BIN" ingest \
        -i "$ACCUMULO_INSTANCE" -z "$ZOOKEEPERS" -u "$USER" -p "$PASSWORD" \
        -c "$CATALOG" -s "$SPEC" -f "$FEATURE_NAME" \
        -C "$CONF" -t "$THREADS" "$split_file" \
    echo "Finished ingesting $split_file"
done

echo "All split files ingested successfully!"
