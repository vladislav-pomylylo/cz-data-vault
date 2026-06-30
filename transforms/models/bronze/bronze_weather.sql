MODEL (
  name bronze.weather,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column timestamp,
    batch_size 1
  ),
  audits (
    not_null(columns := (city, timestamp, temperature_c))
  ),
  owner data,
  cron '@daily',
  description 'Daily weather data for EU capitals from Open-Meteo'
);

SELECT
  city,
  country,
  temperature_c,
  humidity_pct,
  precipitation_mm,
  wind_speed_kmh,
  timestamp,
  _ingested_at
FROM
  raw.weather
WHERE
  timestamp BETWEEN @start_date AND @end_date;
