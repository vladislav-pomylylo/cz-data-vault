MODEL (
  name gold.transport_hub_analytics,
  kind VIEW,
  owner data,
  description 'Transport hub statistics by region'
);

SELECT
  region,
  COUNT(DISTINCT stop_id) AS total_stops,
  COUNT(DISTINCT zone_id) AS total_zones,
  ROUND(AVG(stop_lat), 4) AS avg_latitude,
  ROUND(AVG(stop_lon), 4) AS avg_longitude
FROM silver.transport_stops
GROUP BY region;
