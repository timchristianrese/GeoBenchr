from sedona.spark import SedonaContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, to_timestamp

spark = SparkSession.builder \
    .appName("AIS_to_PostGIS") \
    .getOrCreate()

sedona = SedonaContext.create(spark)

# Read from HDFS
df = spark.read \
    .option("header", "true") \
    .csv("hdfs:///data/ais/raw/*.csv")

# Select + clean
df = df.select(
    col("Timestamp"),
    col("MMSI"),
    col("Latitude"),
    col("Longitude")
)

df = df.filter("Latitude IS NOT NULL AND Longitude IS NOT NULL")

df = df.withColumn(
    "ts",
    to_timestamp("Timestamp", "dd/MM/yyyy HH:mm:ss")
)

df = df.withColumn(
    "geom",
    expr("ST_Point(Longitude, Latitude)")
)

# Write to PostGIS
df.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://{{ postgres_host }}:5432/{{ db_name }}") \
    .option("dbtable", "ais_points") \
    .option("user", "{{ db_user }}") \
    .option("password", "{{ db_password }}") \
    .option("driver", "org.postgresql.Driver") \
    .option("batchsize", 10000) \
    .option("numPartitions", 8) \
    .mode("append") \
    .save()