import geopandas as gpd
import pandas as pd
from pathlib import Path

base_path = Path("/Users/gov/Downloads")

islands_path = base_path / "ne_10m_minor_islands" / "ne_10m_minor_islands.shp"
coastline_path = base_path / "ne_10m_coastline" / "ne_10m_coastline.shp"

bbox = (4.0, 53.0, 17.5, 59.5)

# Read with bbox filter (fast)
gdf_islands = gpd.read_file(islands_path, bbox=bbox)
gdf_coast = gpd.read_file(coastline_path, bbox=bbox)

# Ensure CRS
gdf_islands = gdf_islands.to_crs(4326)
gdf_coast = gdf_coast.to_crs(4326)

# Keep only needed columns
gdf_islands = gdf_islands[["featurecla", "geometry"]]
gdf_coast = gdf_coast[["featurecla", "geometry"]]

# Optional: track source if you still want it (recommended)
gdf_islands["source"] = "islands"
gdf_coast["source"] = "coastline"

# Combine
gdf_combined = gpd.GeoDataFrame(
    pd.concat([gdf_islands, gdf_coast], ignore_index=True),
    crs="EPSG:4326"
)

# Save
output_path = base_path / "combined.parquet"
gdf_combined.to_parquet(output_path)

print(f"Saved to {output_path} with {len(gdf_combined)} features")