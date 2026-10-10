#!/bin/bash
# Sedona: rewrite one HDFS point table as spatially sorted GeoParquet with a bounding-box covering column
# (2026-10-08). The existing files are plain Parquet with the geometry as WKB bytes, decoded in a view, so
# Spark cannot skip anything for a spatial filter. GeoParquet with a per-row bbox column (declared as the
# geometry's "covering") in spatial order lets Sedona skip row groups whose boxes miss the area.
# SORT BY sorts within each output file (no shuffle): a global ORDER BY's shuffle files pushed the worker
# nodes past YARN's 90 % disk limit, which marks them unhealthy and kills the executors.
# Runs in the benchmark client's own Sedona session (GeoParquetBuild.java), where the table views exist.
#   build_geoparquet.sh <table> "<other columns>"   writes hdfs:///geobenchr/<table>.geoparquet (original stays)
set -euo pipefail
T=$1
COLS=$2
D=$HOME/sedona_geoparquet
B=$HOME/bench_sedona
LOG=$D/build.log
echo "$(date -Is) [$T] build start" >> $LOG
/opt/hadoop/bin/hdfs dfs -rm -r -f -skipTrash /geobenchr/$T.geoparquet >/dev/null 2>&1 || true
cd $D
env SUT_HOST=nst-1 SPARK_MASTER=yarn SPARK_HOME=/opt/spark HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop \
    SPARK_CONF="spark.sql.session.timeZone=UTC;spark.dynamicAllocation.enabled=false;spark.executor.instances=36;spark.executor.cores=1;spark.executor.memory=2g;spark.yarn.exclude.nodes=nst-1;spark.sql.shuffle.partitions=400;spark.hadoop.dfs.replication=2" \
  /opt/java/latest/bin/java -Xmx6g \
    --add-exports=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
    --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED \
    --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
    --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED \
    --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED \
    -cp "$D:$B/benchmark_client-run15.jar:$B/deps/*" GeoParquetBuild "
      INSERT OVERWRITE DIRECTORY 'hdfs:///geobenchr/$T.geoparquet' USING geoparquet
        OPTIONS ('geoparquet.covering.geom' 'geom_bbox')
      SELECT $COLS, geom,
             named_struct('xmin', ST_XMin(geom), 'ymin', ST_YMin(geom), 'xmax', ST_XMax(geom), 'ymax', ST_YMax(geom)) AS geom_bbox
      FROM $T
      SORT BY ST_GeoHash(ST_Transform(geom, 'EPSG:3857', 'EPSG:4326'), 10)" > $D/$T.out 2>&1 \
  || { echo "$(date -Is) [$T] build failed, see $T.out" >> $LOG; exit 1; }
echo "$(date -Is) [$T] built ($(grep 'done in' $D/$T.out)): $(/opt/hadoop/bin/hdfs dfs -du -s -h /geobenchr/$T.geoparquet)" >> $LOG
