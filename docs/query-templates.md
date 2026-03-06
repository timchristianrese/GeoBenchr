# Appendix: Query Templates

This document contains the SQL query templates used in the experiments.

## Aviation Queries

### Avi.Q1 — Count Flight Updates in Period
```sql
SELECT f.flightid, COUNT(*)
FROM flight_points f
WHERE f.timestamp BETWEEN :period_medium
GROUP BY f.flightid;
```

### Avi.Q2 — Airport Utilization in Period
```sql
WITH departures AS (
  SELECT f.origin AS airport,
         COUNT(DISTINCT f.flightid) AS departure_count
  FROM flight_trajectories f
  WHERE f.start_time BETWEEN :period_short
  GROUP BY f.origin
),
arrivals AS (
  SELECT f.destination AS airport,
         COUNT(DISTINCT f.flightid) AS arrival_count
  FROM flight_trajectories f
  WHERE f.end_time BETWEEN :period_short
  GROUP BY f.destination
)
SELECT
  COALESCE(d.airport, a.airport) AS airport,
  COALESCE(d.departure_count, 0) AS departures,
  COALESCE(a.arrival_count, 0) AS arrivals,
  COALESCE(d.departure_count, 0)
  + COALESCE(a.arrival_count, 0) AS traffic_count
FROM departures d
FULL JOIN arrivals a
ON d.airport = a.airport;
```

### Avi.Q3 — Flights in Counties
```sql
SELECT k.name,
       COUNT(DISTINCT f.flightid) AS flight_count
FROM counties k
JOIN flight_points f
ON ST_Intersects(f.geom, k.geom)
WHERE k.name = :county
GROUP BY k.name;
```

### Avi.Q4 — Find Flight Closest to Location
```sql
SELECT f.flightid,
       f.airplaneType,
       f.origin,
       f.destination,
       ST_Distance(f.geom, :point) AS min_distance
FROM flight_points f
WHERE ST_DWithin(f.geom, :point, :distance)
ORDER BY min_distance ASC
LIMIT 1;
```

### Avi.Q5 — Flights in County During Period
```sql
SELECT DISTINCT f.flightid
FROM flight_points f, counties k
WHERE k.name = :county
AND f.geom && k.geom
AND f.timestamp BETWEEN :period_medium;
```

### Avi.Q6 — In City Radius During Period
```sql
SELECT f.flightId,
       f.airplanetype,
       f.origin,
       f.destination
FROM cities c
JOIN flight_points f
ON ST_DWithin(f.geom, c.geom, :radius)
WHERE c.name = :city
AND f.timestamp BETWEEN :period_medium;
```

## Cycling Queries

### Cyc.Q1 — Number of Points per Hour
```sql
SELECT EXTRACT(HOUR FROM r.timestamp) AS hour_of_day,
       COUNT(*) AS num_points
FROM ride_points r
WHERE :period_medium @> r.timestamp
GROUP BY hour_of_day
ORDER BY hour_of_day;
```

### Cyc.Q2 — Average Ride Duration
```sql
SELECT AVG(r.end_time - r.start_time) AS avg_duration
FROM ride_trajectories r
WHERE (r.start_time, r.end_time)
OVERLAPS (
  :day::timestamp,
  :day::timestamp + INTERVAL '1 day'
);
```

### Cyc.Q3 — Cycling Trips in Districts
```sql
SELECT d.name,
       COUNT(DISTINCT r.ride_id) AS ride_count
FROM berlin_districts d
JOIN ride_points r
ON ST_Intersects(r.geom, d.geom)
WHERE d.name = :district
GROUP BY d.name;
```

### Cyc.Q4 — Rides Close to University
```sql
SELECT DISTINCT r.ride_id
FROM ride_points r
JOIN universities u
ON ST_DWithin(r.geom, u.geom, :distance)
WHERE u.name = :university;
```

### Cyc.Q5 — Average Ride Duration to University
```sql
SELECT AVG(r.end_time - r.start_time) AS avg_duration
FROM ride_trajectories r
JOIN universities u
ON ST_DWithin(r.destination_geom, u.geom, :distance)
WHERE :period_medium @> r.start_time::timestamp
AND u.name = :university;
```

### Cyc.Q6 — Rides Through Multiple Districts
```sql
SELECT DISTINCT r.ride_id
FROM ride_points r
JOIN berlin_districts d
ON ST_Intersects(r.geom, d.geom)
WHERE :period_medium @> r.timestamp
GROUP BY r.ride_id
HAVING COUNT(DISTINCT d.name) > 1;
```

## AIS Queries

### AIS.Q1 — Count Active Vessels
```sql
SELECT COUNT(DISTINCT c.crossing_id)
FROM crossing_trajectories c
WHERE (c.start_time, c.end_time)
OVERLAPS :period_medium;
```

### AIS.Q2 — Crossings Active by Hour
```sql
SELECT COUNT(DISTINCT c.crossing_id)
FROM crossing_points c
WHERE EXTRACT(HOUR FROM c.timestamp) = :hour
AND :period_medium @> c.timestamp;
```

### AIS.Q3 — Active Crossings Near Island
```sql
SELECT COUNT(DISTINCT crossing_id)
FROM crossing_points c
JOIN islands i
ON ST_DWithin(c.geom, i.geom, :distance)
WHERE i.name = :island
GROUP BY i.name;
```

### AIS.Q4 — Crossings Between Harbors
```sql
SELECT DISTINCT c.crossing_id
FROM crossing_trajectories c
JOIN harbors h_start
ON h_start.name = :harbor
AND ST_DWithin(
  c.origin_geom::geometry,
  h_start.geom::geometry,
  :distance
)
JOIN harbors h_end
ON h_end.name = :port
AND ST_DWithin(
  c.destination_geom::geometry,
  h_end.geom::geometry,
  :distance
);
```

### AIS.Q5 — Average Crossing Duration
```sql
SELECT AVG(c.end_time - c.start_time) AS avg_duration
FROM crossing_trajectories c
JOIN harbors h
ON ST_DWithin(
  c.origin_geom::geometry,
  h.geom::geometry,
  :radius
)
WHERE h.name = :harbor
AND :period_medium @> c.start_time::timestamp;
```

### AIS.Q6 — Harbor Activity
```sql
SELECT h.name AS harbor_name,
       COUNT(DISTINCT c.crossing_id)
       AS unique_crossings_near_harbor
FROM harbors h
JOIN crossing_points c
ON ST_DWithin(c.geom, h.geom, :radius)
WHERE :period_medium @> c.timestamp
AND h.name = :harbor
GROUP BY h.name;
```
