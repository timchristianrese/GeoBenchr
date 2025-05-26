import geopandas as gpd

# Try reading the file
gdf = gpd.read_parquet('../processed/ais/point0.geoparquet')

# Basic check: print CRS and geometry
print(gdf.crs)
print(gdf.geometry.head())
