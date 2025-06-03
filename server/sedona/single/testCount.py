from sedona.register import SedonaRegistrator
from sedona.utils import SedonaKryoRegistrator, KryoSerializer
from sedona.sql.types import GeometryType
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
from sedona.spark import *


config = (
    SedonaContext.builder()
    .config(
        "spark.jars.packages",
        "org.apache.sedona:sedona-spark-3.3_2.12:1.7.1,"
        "org.datasyslab:geotools-wrapper:1.7.1-28.5",
    )
    .config(
        "spark.jars.repositories",
        "https://artifacts.unidata.ucar.edu/repository/unidata-all",
    )
    .getOrCreate()
)
sedona = SedonaContext.create(config)


# Path to input CSV files (adjust path as needed)
csv_path = "../data/cycling/trip*.csv"

# Read CSV files with header; assume WKT is in 3rd column (index 2)
df = sedona.read.option("header", "true").csv(csv_path)

# Create a new DataFrame with geometry column from WKT (column 3)
# Replace df.columns[2] with actual column name if known
wkt_column_name = df.columns[2]

df_with_geom = df.withColumn(
    "geom",
    expr(f"ST_GeomFromWKT({wkt_column_name})").cast(GeometryType())
)

# Register as a temp SQL view for running queries
df_with_geom.createOrReplaceTempView("trip_data")

# Run a simple count query
#work these into the data 
# ==========================
# 🔄 REAL-TIME QUERIES
# ==========================

# S1 (Real-Time Spatial): Is a given trip currently within a high-risk (accident-prone) zone?
sedona.sql("SELECT trip_id FROM cycling_trips ct, high_risk_zones hz WHERE ST_Intersects(ct.geom, hz.zone_geom) AND ct.start_time >= current_timestamp() - interval 5 minutes")

# T1 (Real-Time Temporal): Are there anomalies in the number of trips in the last 10 minutes?
sedona.sql("SELECT COUNT(*) AS trip_count_10min FROM cycling_trips WHERE start_time >= current_timestamp() - interval 10 minutes")

# ST1 (Real-Time Spatiotemporal): Which bikes are currently in the Tiergarten park area?
sedona.sql("SELECT bike_id FROM cycling_trips ct, parks p WHERE p.name = 'Tiergarten' AND ST_Intersects(ct.geom, p.geom) AND ct.end_time IS NULL AND ct.start_time >= current_timestamp() - interval 1 hour")

# ST3 (Real-Time Spatiotemporal): Has there been a sudden drop in usage in a specific district today?
sedona.sql("SELECT d.district_name, COUNT(*) AS trips_today FROM cycling_trips ct, berlin_districts d WHERE ST_Contains(d.geom, ct.start_point) AND DATE(start_time) = CURRENT_DATE() GROUP BY d.district_name")


# S2 (Analytic Spatial): Which areas have the densest network of completed trips over a month?
sedona.sql("SELECT grid_cell_id, COUNT(*) AS trip_count FROM (SELECT ST_GeomFromWKT(ct.geom) AS geom, ST_Pixelize(ct.geom, 0.01) AS grid_cell_id FROM cycling_trips ct WHERE start_time BETWEEN '2024-09-01' AND '2024-09-30') grid GROUP BY grid_cell_id ORDER BY trip_count DESC LIMIT 10")

# T2 (Analytic Temporal): What is the average trip duration trend per weekday over the last year?
sedona.sql("SELECT DAYOFWEEK(start_time) AS weekday, AVG(trip_duration) AS avg_duration FROM cycling_trips WHERE start_time >= '2024-01-01' GROUP BY DAYOFWEEK(start_time) ORDER BY weekday")

# ST2 (Analytic Spatiotemporal): What are the top origin-destination pairs during weekday rush hours?
sedona.sql("SELECT start_station_id, end_station_id, COUNT(*) AS trip_count FROM cycling_trips WHERE HOUR(start_time) BETWEEN 7 AND 9 AND DAYOFWEEK(start_time) BETWEEN 2 AND 6 GROUP BY start_station_id, end_station_id ORDER BY trip_count DESC LIMIT 10")

# ST4 (Analytic Spatiotemporal): What is the average speed of trips in downtown Berlin per season?
sedona.sql("SELECT season, AVG(trip_distance / (trip_duration / 3600.0)) AS avg_speed_kph FROM (SELECT *, CASE WHEN MONTH(start_time) IN (12, 1, 2) THEN 'Winter' WHEN MONTH(start_time) IN (3, 4, 5) THEN 'Spring' WHEN MONTH(start_time) IN (6, 7, 8) THEN 'Summer' ELSE 'Fall' END AS season FROM cycling_trips ct, downtown_zones dz WHERE ST_Intersects(ct.geom, dz.geom)) seasonal_trips GROUP BY season")


# Show result
result.show()

# Stop SparkSession
sedona.stop()
