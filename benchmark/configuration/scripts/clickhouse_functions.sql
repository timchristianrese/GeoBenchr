-- SQL functions used by the ClickHouse benchmark queries. Create once per cluster:
--   clickhouse-client --port 9500 --multiquery < clickhouse_functions.sql
-- They are lambda macros: ClickHouse expands them into the query text before running it, so they
-- cost the same as writing the expression out.

-- EPSG:3857 (Web Mercator) projection. The point tables store x/y as MATERIALIZED columns with
-- exactly this formula (scripts/ch_add_xy.sh); the functions remain for the reference geometry.
CREATE FUNCTION IF NOT EXISTS webMercatorX ON CLUSTER cluster_1 AS lon -> (6378137. * radians(lon));
CREATE FUNCTION IF NOT EXISTS webMercatorY ON CLUSTER cluster_1 AS lat -> (6378137. * log(tan((pi() / 4) + (radians(lat) / 2))));
CREATE FUNCTION IF NOT EXISTS webMercatorDistance ON CLUSTER cluster_1 AS (lon1, lat1, lon2, lat2) ->
  sqrt(pow(webMercatorX(lon1) - webMercatorX(lon2), 2) + pow(webMercatorY(lat1) - webMercatorY(lat2), 2));

-- H3 cell covers (2026-10-07). The point tables are sorted by h3 = geoToH3(lat, lon, 9), so a
-- query that filters on h3 lets ClickHouse skip every granule outside the listed cells (the
-- primary-key index); the query's exact spatial predicate stays in place and decides the result.
-- A cover must therefore be a SUPERSET of the res-9 cells any matching point can lie in:
--
--   1. coarse cells (res 7, ~5 km^2) of the shape: cells whose centre lies inside it
--      (h3PolygonToCells) plus the cells of its outline, densified to a point every 0.005 deg
--      (well under a res-7 cell) so no stretch of boundary is skipped;
--   2. widened by k rings, k = ceil(d / 1000) + 2: one ring per 1000 m of search distance d (a
--      res-7 ring is ~2 km wide, so 1000 m is conservative) plus 2 for cells the outline only
--      grazes. d is passed in EPSG:3857 units, which are >= true metres (3857 = metres / cos(lat)),
--      so this over- rather than under-covers;
--   3. expanded to the res-9 children the tables store.
--
-- Shapes are (lon, lat) tuples, like pointInPolygon's.

-- every point along a path of (lon, lat) tuples, with a point at least every 0.005 deg
CREATE FUNCTION IF NOT EXISTS h3DensifyPath ON CLUSTER cluster_1 AS (path) ->
  arrayPushBack(
    arrayFlatten(arrayMap((a, b) -> arrayMap(
        i -> (a.1 + (b.1 - a.1) * i / (toUInt32(ceil(greatest(abs(b.1 - a.1), abs(b.2 - a.2)) / 0.005)) + 1),
              a.2 + (b.2 - a.2) * i / (toUInt32(ceil(greatest(abs(b.1 - a.1), abs(b.2 - a.2)) / 0.005)) + 1)),
        range(toUInt32(ceil(greatest(abs(b.1 - a.1), abs(b.2 - a.2)) / 0.005)) + 1)),
      arrayPopBack(path), arrayPopFront(path))),
    path[-1]);

-- res-9 cells within d (EPSG:3857 units) of a set of res-7 seed cells
CREATE FUNCTION IF NOT EXISTS h3CoverCells ON CLUSTER cluster_1 AS (seeds, d) ->
  arrayDistinct(arrayFlatten(arrayMap(c -> h3ToChildren(c, 9),
    arrayDistinct(arrayFlatten(arrayMap(c -> h3kRing(c, toUInt16(ceil(d / 1000) + 2)), arrayDistinct(seeds)))))));

-- within d of a point
CREATE FUNCTION IF NOT EXISTS h3CoverPoint ON CLUSTER cluster_1 AS (lon, lat, d) ->
  h3CoverCells([geoToH3(lat, lon, 7)], d);

-- inside, or within d of, a closed ring (first point = last point not required)
CREATE FUNCTION IF NOT EXISTS h3CoverRing ON CLUSTER cluster_1 AS (ring, d) ->
  h3CoverCells(arrayConcat(
      h3PolygonToCells(ring, 7),
      arrayMap(p -> geoToH3(p.2, p.1, 7), h3DensifyPath(arrayPushBack(ring, ring[1])))), d);

-- within d of an open line
CREATE FUNCTION IF NOT EXISTS h3CoverLine ON CLUSTER cluster_1 AS (line, d) ->
  h3CoverCells(arrayMap(p -> geoToH3(p.2, p.1, 7), h3DensifyPath(line)), d);
