"""Sedona's ST_Transform needs explicit CRS strings (2026-10-08).

The :point / :region parameters are rendered in PostGIS syntax, ST_Transform(ST_SetSRID(<geom>, 4326), 3857).
Sedona rejects the bare target code ("No authority was defined for code \"3857\""), so every Sedona query
using one of these parameters failed. Sedona takes ST_Transform(geom, sourceCRS, targetCRS), and in
Sedona >= 1.5 that assumes lon/lat axis order, like PostGIS (checked: same coordinates as PostGIS).
"""
import re

_PATTERN = re.compile(r"ST_Transform\(ST_SetSRID\((ST_(?:Point|MakeEnvelope)\([^()]*\)),\s*4326\),\s*3857\)")


def fix_text(text):
    return _PATTERN.sub(r"ST_Transform(\1, 'EPSG:4326', 'EPSG:3857')", text)


def fix_file(path):
    with open(path) as f:
        text = f.read()
    with open(path, "w") as f:
        f.write(fix_text(text))
