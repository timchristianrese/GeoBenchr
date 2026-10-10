import pyarrow.parquet as pq
datapath = "/Users/gov/Prog/git/GeoBenchr/data_generators/cycling/output/parquet/trajectories_batch_0.parquet"

table = pq.read_table(datapath, columns=["trip_id", "rider_id", "lat", "lon", "timestamp"])
df = table.to_pandas()
#check year values of df
print(df.head())