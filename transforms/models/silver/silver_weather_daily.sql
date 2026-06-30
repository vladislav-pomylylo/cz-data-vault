MODEL (
  name silver.weather_daily,
  kind VIEW,
  owner data,
  description 'Daily weather aggregated from current readings'
);

SELECT
  city,
  country,
  DATE(timestamp) AS date,
  AVG(temperature_c) AS avg_temperature_c,
  AVG(humidity_pct) AS avg_humidity_pct,
  SUM(precipitation_mm) AS total_precipitation_mm,
  AVG(wind_speed_kmh) AS avg_wind_speed_kmh
FROM bronze.weather
GROUP BY city, country, DATE(timestamp);
