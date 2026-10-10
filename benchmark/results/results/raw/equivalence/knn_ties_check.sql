SELECT 'geolife/ClosestRestaurantToPoint rep3 h3', groupArray(d), groupArray(id) FROM (SELECT round(sqrt(pow(webMercatorX(lon) - webMercatorX(116.32233606307769), 2) + pow(webMercatorY(lat) - webMercatorY(39.807099482561426), 2)), 4) AS d, id FROM (SELECT *
FROM geolife_points
WHERE h3 GLOBAL IN (SELECT arrayJoin(h3CoverPoint(116.32233606307769, 39.807099482561426, 5000)))
  AND venue_type = 'Restaurant' AND sqrt(pow(x - 6378137. * radians(116.32233606307769), 2) + pow(y - 6378137. * log(tan(pi() / 4 + radians(39.807099482561426) / 2)), 2)) <= 5000
ORDER BY sqrt(pow(x - 6378137. * radians(116.32233606307769), 2) + pow(y - 6378137. * log(tan(pi() / 4 + radians(39.807099482561426) / 2)), 2))
LIMIT 1) ORDER BY d, id);
SELECT 'geolife/ClosestRestaurantToPoint rep3 scan', groupArray(d), groupArray(id) FROM (SELECT round(sqrt(pow(webMercatorX(lon) - webMercatorX(116.32233606307769), 2) + pow(webMercatorY(lat) - webMercatorY(39.807099482561426), 2)), 4) AS d, id FROM (SELECT *
FROM geolife_points
WHERE venue_type = 'Restaurant' AND sqrt(pow(x - 6378137. * radians(116.32233606307769), 2) + pow(y - 6378137. * log(tan(pi() / 4 + radians(39.807099482561426) / 2)), 2)) <= 5000
ORDER BY sqrt(pow(x - 6378137. * radians(116.32233606307769), 2) + pow(y - 6378137. * log(tan(pi() / 4 + radians(39.807099482561426) / 2)), 2))
LIMIT 1) ORDER BY d, id);
SELECT 'geolife/ClosestRestaurantToPoint rep4 h3', groupArray(d), groupArray(id) FROM (SELECT round(sqrt(pow(webMercatorX(lon) - webMercatorX(116.19761065108271), 2) + pow(webMercatorY(lat) - webMercatorY(39.88863886714664), 2)), 4) AS d, id FROM (SELECT *
FROM geolife_points
WHERE h3 GLOBAL IN (SELECT arrayJoin(h3CoverPoint(116.19761065108271, 39.88863886714664, 5000)))
  AND venue_type = 'Restaurant' AND sqrt(pow(x - 6378137. * radians(116.19761065108271), 2) + pow(y - 6378137. * log(tan(pi() / 4 + radians(39.88863886714664) / 2)), 2)) <= 5000
ORDER BY sqrt(pow(x - 6378137. * radians(116.19761065108271), 2) + pow(y - 6378137. * log(tan(pi() / 4 + radians(39.88863886714664) / 2)), 2))
LIMIT 1) ORDER BY d, id);
SELECT 'geolife/ClosestRestaurantToPoint rep4 scan', groupArray(d), groupArray(id) FROM (SELECT round(sqrt(pow(webMercatorX(lon) - webMercatorX(116.19761065108271), 2) + pow(webMercatorY(lat) - webMercatorY(39.88863886714664), 2)), 4) AS d, id FROM (SELECT *
FROM geolife_points
WHERE venue_type = 'Restaurant' AND sqrt(pow(x - 6378137. * radians(116.19761065108271), 2) + pow(y - 6378137. * log(tan(pi() / 4 + radians(39.88863886714664) / 2)), 2)) <= 5000
ORDER BY sqrt(pow(x - 6378137. * radians(116.19761065108271), 2) + pow(y - 6378137. * log(tan(pi() / 4 + radians(39.88863886714664) / 2)), 2))
LIMIT 1) ORDER BY d, id);
SELECT 'geolife/ClosestRestaurantToPoint rep5 h3', groupArray(d), groupArray(id) FROM (SELECT round(sqrt(pow(webMercatorX(lon) - webMercatorX(116.24284583777377), 2) + pow(webMercatorY(lat) - webMercatorY(40.024807555857045), 2)), 4) AS d, id FROM (SELECT *
FROM geolife_points
WHERE h3 GLOBAL IN (SELECT arrayJoin(h3CoverPoint(116.24284583777377, 40.024807555857045, 5000)))
  AND venue_type = 'Restaurant' AND sqrt(pow(x - 6378137. * radians(116.24284583777377), 2) + pow(y - 6378137. * log(tan(pi() / 4 + radians(40.024807555857045) / 2)), 2)) <= 5000
ORDER BY sqrt(pow(x - 6378137. * radians(116.24284583777377), 2) + pow(y - 6378137. * log(tan(pi() / 4 + radians(40.024807555857045) / 2)), 2))
LIMIT 1) ORDER BY d, id);
SELECT 'geolife/ClosestRestaurantToPoint rep5 scan', groupArray(d), groupArray(id) FROM (SELECT round(sqrt(pow(webMercatorX(lon) - webMercatorX(116.24284583777377), 2) + pow(webMercatorY(lat) - webMercatorY(40.024807555857045), 2)), 4) AS d, id FROM (SELECT *
FROM geolife_points
WHERE venue_type = 'Restaurant' AND sqrt(pow(x - 6378137. * radians(116.24284583777377), 2) + pow(y - 6378137. * log(tan(pi() / 4 + radians(40.024807555857045) / 2)), 2)) <= 5000
ORDER BY sqrt(pow(x - 6378137. * radians(116.24284583777377), 2) + pow(y - 6378137. * log(tan(pi() / 4 + radians(40.024807555857045) / 2)), 2))
LIMIT 1) ORDER BY d, id);
SELECT 'geolife/kNN rep1 h3', groupArray(d), groupArray(id) FROM (SELECT round(sqrt(pow(webMercatorX(lon) - webMercatorX(116.33578863684798), 2) + pow(webMercatorY(lat) - webMercatorY(39.81956404118515), 2)), 4) AS d, id FROM (SELECT id, lon, lat, timestamp
FROM geolife_points
WHERE h3 GLOBAL IN (SELECT arrayJoin(h3CoverPoint(116.33578863684798, 39.81956404118515, 502.26989567440677)))
  AND sqrt(pow(x - 6378137. * radians(116.33578863684798), 2) + pow(y - 6378137. * log(tan(pi() / 4 + radians(39.81956404118515) / 2)), 2)) <= 502.26989567440677
ORDER BY sqrt(pow(x - 6378137. * radians(116.33578863684798), 2) + pow(y - 6378137. * log(tan(pi() / 4 + radians(39.81956404118515) / 2)), 2))
LIMIT 5) ORDER BY d, id);
SELECT 'geolife/kNN rep1 scan', groupArray(d), groupArray(id) FROM (SELECT round(sqrt(pow(webMercatorX(lon) - webMercatorX(116.33578863684798), 2) + pow(webMercatorY(lat) - webMercatorY(39.81956404118515), 2)), 4) AS d, id FROM (SELECT id, lon, lat, timestamp
FROM geolife_points
WHERE sqrt(pow(x - 6378137. * radians(116.33578863684798), 2) + pow(y - 6378137. * log(tan(pi() / 4 + radians(39.81956404118515) / 2)), 2)) <= 502.26989567440677
ORDER BY sqrt(pow(x - 6378137. * radians(116.33578863684798), 2) + pow(y - 6378137. * log(tan(pi() / 4 + radians(39.81956404118515) / 2)), 2))
LIMIT 5) ORDER BY d, id);
SELECT 'geolife/kNN rep2 h3', groupArray(d), groupArray(id) FROM (SELECT round(sqrt(pow(webMercatorX(lon) - webMercatorX(116.3129108736583), 2) + pow(webMercatorY(lat) - webMercatorY(39.87629327927674), 2)), 4) AS d, id FROM (SELECT id, lon, lat, timestamp
FROM geolife_points
WHERE h3 GLOBAL IN (SELECT arrayJoin(h3CoverPoint(116.3129108736583, 39.87629327927674, 1261.4295515414744)))
  AND sqrt(pow(x - 6378137. * radians(116.3129108736583), 2) + pow(y - 6378137. * log(tan(pi() / 4 + radians(39.87629327927674) / 2)), 2)) <= 1261.4295515414744
ORDER BY sqrt(pow(x - 6378137. * radians(116.3129108736583), 2) + pow(y - 6378137. * log(tan(pi() / 4 + radians(39.87629327927674) / 2)), 2))
LIMIT 1) ORDER BY d, id);
SELECT 'geolife/kNN rep2 scan', groupArray(d), groupArray(id) FROM (SELECT round(sqrt(pow(webMercatorX(lon) - webMercatorX(116.3129108736583), 2) + pow(webMercatorY(lat) - webMercatorY(39.87629327927674), 2)), 4) AS d, id FROM (SELECT id, lon, lat, timestamp
FROM geolife_points
WHERE sqrt(pow(x - 6378137. * radians(116.3129108736583), 2) + pow(y - 6378137. * log(tan(pi() / 4 + radians(39.87629327927674) / 2)), 2)) <= 1261.4295515414744
ORDER BY sqrt(pow(x - 6378137. * radians(116.3129108736583), 2) + pow(y - 6378137. * log(tan(pi() / 4 + radians(39.87629327927674) / 2)), 2))
LIMIT 1) ORDER BY d, id);
SELECT 'geolife/kNN rep5 h3', groupArray(d), groupArray(id) FROM (SELECT round(sqrt(pow(webMercatorX(lon) - webMercatorX(116.58120934065653), 2) + pow(webMercatorY(lat) - webMercatorY(39.91265824790936), 2)), 4) AS d, id FROM (SELECT id, lon, lat, timestamp
FROM geolife_points
WHERE h3 GLOBAL IN (SELECT arrayJoin(h3CoverPoint(116.58120934065653, 39.91265824790936, 921.6119454529043)))
  AND sqrt(pow(x - 6378137. * radians(116.58120934065653), 2) + pow(y - 6378137. * log(tan(pi() / 4 + radians(39.91265824790936) / 2)), 2)) <= 921.6119454529043
ORDER BY sqrt(pow(x - 6378137. * radians(116.58120934065653), 2) + pow(y - 6378137. * log(tan(pi() / 4 + radians(39.91265824790936) / 2)), 2))
LIMIT 5) ORDER BY d, id);
SELECT 'geolife/kNN rep5 scan', groupArray(d), groupArray(id) FROM (SELECT round(sqrt(pow(webMercatorX(lon) - webMercatorX(116.58120934065653), 2) + pow(webMercatorY(lat) - webMercatorY(39.91265824790936), 2)), 4) AS d, id FROM (SELECT id, lon, lat, timestamp
FROM geolife_points
WHERE sqrt(pow(x - 6378137. * radians(116.58120934065653), 2) + pow(y - 6378137. * log(tan(pi() / 4 + radians(39.91265824790936) / 2)), 2)) <= 921.6119454529043
ORDER BY sqrt(pow(x - 6378137. * radians(116.58120934065653), 2) + pow(y - 6378137. * log(tan(pi() / 4 + radians(39.91265824790936) / 2)), 2))
LIMIT 5) ORDER BY d, id);
