# YAML -> Query Generators (PostGIS / Sedona / SpaceTime / MongoDB)

## Project Goal
This project allows you to describe a **spatio-temporal query once** in a unified YAML format, and automatically generate equivalent queries for different backends:

- **PostGIS** (SQL/PostgreSQL)
- **Apache Sedona** (PySpark + Sedona SQL functions)
- **SpaceTime DB** (TU Berlin)
- **MongoDB** (Python Aggregation Pipeline)

The core idea:  
**One YAML -> All executable queries.**

---

## Project Structure
- `queryConfigGenerator.py` -> YAML parser, builds a `QueryConfig` object tree (`Filter`, `TimeFilter`, `SpatialFilter`, etc.).
- `PostGISGenerator.py` -> renders SQL for PostGIS.
- `SedonaGenerator.py` -> renders PySpark code with Sedona functions.
- `SpaceTimeGenerator.py` -> renders SQL for SpaceTime.
- `mongoDBGenerator.py` -> renders Python code that executes a MongoDB aggregation pipeline.
<!-- - `tests/` -> examples and integration tests. -->

---

## How to run (generateQueries.py)
`python generateQueries.py [-p] [-m] [-s] [-st] [-r] CONFIG`
- **Pick generators**: `-p/--postgis`, `-m/--mongo`, `-s/--sedona`, `-st/--spacetime` (choose at least one).
- **CONFIG**: a YAML file or directory containing `*.yml | *.yaml` (use `-r/--recursive` to scan subfolders).
- **Output**: code is written to `queries/<yaml-name>/` (auto-suffixed if the folder exists). Each YAML query becomes a subfolder `<query-name>/` with the generated files:
  - `postgis.sql`, `spacetime.sql`, `mongodb.py`, `sedona.py`
  - an `INDEX.txt` is created what was generated.

**Examples**
- Generate PostGIS + MongoDB for one file:
`python generateQueries.py -p -m example.yaml`
- Generate all backends for an entire folder (recursively):
`python generateQueries.py -p -m -s -st -r queries_direcotry/`

---

## Supported Features
- **Sources**
  - Standard database table (`type: table`)
  - CSV input with parsing options (`delimiter`, `header`, `schema`, …) (hard to use)
  - WKT geometry columns (via `format: wkt:gpoint`, `wkt:gmpolygon`, etc.)
- **Selection**
  - Raw column selection (`select`)
  - Aggregations (`COUNT`, `SUM`, `AVG`, …)
  - Aggregations with `DISTINCT`
    - Express in YAML as:
      ```yaml
      aggregation:
        - function: COUNT
          field: "DISTINCT <alias.field>"
          alias: <alias>
      ```
- **Filters**
  - Simple filters: `=`, `!=`, `<`, `>`, `>=`, `<=`, `IN`, `NOT IN`, `IS NULL`
  - Temporal filters (`time_filter`) with `start` / `end`
  - Spatial filters (`spatial_filter`) such as:
    - `intersects`, `contains`, `within`, `overlaps`, `touches`, `dwithin`, …
    - `dwithin` supports both meters (`m`) and degrees (`deg`)
- **Logical combinations**
  - `and`, `or`, `not`
- **Joins**
  - Inner / Left / Right / Full
  - Join clauses can include simple, temporal, or spatial filters
- **WITH clauses (CTEs)** for reusable subqueries
- **Group By / Having / Order By**
- **Limit / Offset**

---

## Important Details & Subtleties
- **PostGIS**
  - String literals **must be quoted** (`'Paris'`).  
  - Supported geometry generation: `POINT`, `POLYGON`, `LINESTRING`, `MULTIPOLYGON`.
- **Sedona (PySpark)**
  - `WAREHOUSE_DIR` is currently **hardcoded** in the header bootstrap.  
    Needs to be made configurable.
  - If a string literal is not quoted, the generator raises an error.
- **SpaceTime DB**
  - WKT geometry columns must declare a `format` in the YAML schema (`wkt:gpoint`, `wkt:gmpolygon`, …).
  - Temporal filters require UTC instants/ranges in the form:  
    ```
    '[YYYY-MM-DD HH:MM:SS, YYYY-MM-DD HH:MM:SS]'
    ```
    Both naive UTC and `Z` are accepted.  
    Offsets like `+02:00` are rejected.
  - Optimization: for point-in-polygon checks, the generator emits `point <@ polygon` instead of a generic `ST_Intersects`.
- **MongoDB**
  - The MongoDB generator emits a self-contained Python script that builds the aggregation pipeline and prints results.
  - Ensure your MongoDB collections have the expected schema and any required `2dsphere` indexes for spatial queries.
  - Spatial predicates are approximations of SQL/GEOS behavior: MongoDB uses GeoJSON with `2dsphere` indexes and spherical math. Complex predicates (`within/contains/intersects/dwithin`) are mapped to the closest `$geoWithin / $geoIntersects / $near` operators; results can slightly differ from exact geodesic or topological semantics.

---

## Example YAML

This YAML counts the number of active rides in a given one-hour period:

```yaml
name: countActiveRidesInPeriod
source: "ride_points r"
source_data:
  type: table
select: []
aggregations:
  - function: COUNT
    field: "DISTINCT r.trip_id"
filter:
  time_filter:
    - field: "r.t"
      start: "'2022-10-04T18:00:00Z'"
      end:   "'2022-10-04T19:00:00Z'"
```