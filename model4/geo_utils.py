import json
import numpy as np
import torch
from pathlib import Path
from shapely.geometry import shape
from shapely import vectorized as SV
from scipy.spatial import KDTree

# ── load region polygon ──
REGION_PATH = Path(__file__).resolve().parent / "region_border.geojson"

with open(REGION_PATH, "r") as f:
    geo = json.load(f)

geom = shape(geo["geometry"] if "geometry" in geo else geo["features"][0]["geometry"])

# boundary coordinates (lon, lat)
if geom.type == "Polygon":
    border_lonlat = np.array(geom.exterior.coords)
else:
    border_lonlat = np.vstack([np.array(p.exterior.coords) for p in geom.geoms])

# KD‑tree for fast nearest border point search
# store as (lat, lon) for consistency with the rest of the pipeline
KD_TREE = KDTree(np.column_stack((border_lonlat[:, 1], border_lonlat[:, 0])))

def border_coords():
    """Return np.array (N,2) [lon, lat] for random border point sampling."""
    return border_lonlat.copy()

def signed_distance_torch(lat_t: torch.Tensor, lon_t: torch.Tensor) -> torch.Tensor:
    """
    Compute signed distance from the region border
    return distance in meters (negative inside region, positive outside)
    """
    shape_in = lat_t.shape
    lat_np = lat_t.reshape(-1).cpu().numpy()
    lon_np = lon_t.reshape(-1).cpu().numpy()

    # approx distance in degrees, then converted to meters
    dist_deg, _ = KD_TREE.query(np.column_stack((lat_np, lon_np)))
    dist_km = dist_deg * 111.2
    dist_m = dist_km * 1000

    # sign: negative if inside the region
    inside = SV.contains(geom, lon_np, lat_np)
    dist_m = np.where(inside, -dist_m, dist_m)

    return torch.tensor(dist_m.reshape(shape_in), dtype=torch.float32, device=lat_t.device)
