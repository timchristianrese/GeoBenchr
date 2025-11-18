import pandas as pd
import glob
import os
from shapely import wkt
from shapely.geometry import MultiPolygon, Polygon

# Pattern to find all target CSVs two levels down (e.g. ../*/config/*-wkt.csv)
input_pattern = os.path.join("..", "*", "data", "*-wkt.csv")

# Find all matching files
csv_files = glob.glob(input_pattern)

print(f"Found {len(csv_files)} CSV files to simplify:")

for f in csv_files:
    print(f"  - {f}")

def simplify_to_max_points(geom_wkt, max_points=10, start_tol=0.0001, tol_step=0.0005):
    """Simplify a polygon or multipolygon until it has <= max_points vertices."""
    try:
        geom = wkt.loads(geom_wkt)
    except Exception:
        return geom_wkt  # skip malformed WKT

    def simplify_polygon(poly):
        tol = start_tol
        simplified = poly.simplify(tol, preserve_topology=True)
        while len(simplified.exterior.coords) > max_points and tol < 1:
            tol += tol_step
            simplified = poly.simplify(tol, preserve_topology=True)
        return simplified

    if isinstance(geom, MultiPolygon):
        simplified_polys = [simplify_polygon(p) for p in geom.geoms]
        simplified_geom = MultiPolygon(simplified_polys)
    elif isinstance(geom, Polygon):
        simplified_geom = simplify_polygon(geom)
    else:
        return geom_wkt

    return simplified_geom.wkt

# Process each file
for csv_file in csv_files:
    print(f"\nProcessing: {csv_file}")
    try:
        df = pd.read_csv(csv_file, sep=';')
        if "polygon" not in df.columns:
            print("  ⚠️  Skipping — no 'polygon' column found.")
            continue

        df["polygon"] = df["polygon"].apply(lambda g: simplify_to_max_points(g, max_points=10))

        # Output filename: same folder, with "-simplified.csv" suffix
        base, ext = os.path.splitext(csv_file)
        output_file = f"{base}-simplified.csv"

        df.to_csv(output_file, sep=';', index=False)
        print(f"  ✅ Saved simplified file as: {output_file}")
    except Exception as e:
        print(f"  ❌ Error processing {csv_file}: {e}")

print("\nAll done!")
