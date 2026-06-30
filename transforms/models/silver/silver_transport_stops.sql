MODEL (
  name silver.transport_stops,
  kind VIEW,
  owner data,
  description 'GTFS stops with region assignment'
);

SELECT
  stop_id,
  stop_name,
  stop_lat,
  stop_lon,
  zone_id,
  CASE
    WHEN stop_lat BETWEEN 49.9 AND 50.2
     AND stop_lon BETWEEN 14.2 AND 14.7 THEN 'Prague Centre'
    WHEN stop_lat BETWEEN 50.0 AND 50.2
     AND stop_lon BETWEEN 14.3 AND 14.6 THEN 'Prague Outer'
    ELSE 'Prague Region'
  END AS region
FROM bronze.gtfs_stops
WHERE stop_lat IS NOT NULL AND stop_lon IS NOT NULL
  AND stop_lat BETWEEN 49.8 AND 50.3
  AND stop_lon BETWEEN 14.0 AND 14.9;
