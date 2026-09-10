import pandas as pd
from shapely import wkt
from shapely.geometry import Polygon, MultiPolygon


def parse_geometry(geom_str):
    """
    Convert WKT string to shapely geometry.
    Handles Polygon and MultiPolygon.
    """
    if pd.isna(geom_str):
        return None

    try:
        geom = wkt.loads(geom_str)

        if isinstance(geom, (Polygon, MultiPolygon)):
            return geom

        raise ValueError(f"Unsupported geometry type: {type(geom)}")

    except Exception as e:
        raise ValueError(f"Invalid WKT: {geom_str[:50]}... -> {e}")


def csv_to_parquet(input_csv, output_parquet):
    df = pd.read_csv(input_csv, delimiter=";", encoding="utf-8")  # Adjust delimiter if needed
    if "name" not in df.columns or "polygon" not in df.columns:
        raise ValueError("CSV must contain 'name' and 'polygon' columns")

    # Parse geometries
    df["geometry"] = df["polygon"].apply(parse_geometry)

    # Drop original text column if you want
    df = df.drop(columns=["polygon"])

    # Convert shapely objects to WKB for efficient Parquet storage
    df["geometry"] = df["geometry"].apply(lambda g: g.wkb if g else None)

    # Write to Parquet
    df.to_parquet(output_parquet, engine="pyarrow", index=False)

    print(f"Saved Parquet to {output_parquet}")


if __name__ == "__main__":
    import sys

    if len(sys.argv) != 3:
        print("Usage: python convert.py input.csv output.parquet")
        sys.exit(1)

    csv_to_parquet(sys.argv[1], sys.argv[2])