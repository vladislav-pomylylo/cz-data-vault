MODEL (
  name gold.city_economic_index,
  kind VIEW,
  owner data,
  description 'Composite economic index by city: GDP, inflation, unemployment, weather overlay'
);

WITH city_map AS (
  SELECT 'Prague' AS city, 'CZ' AS country_code
  UNION ALL SELECT 'Vienna', 'AT'
  UNION ALL SELECT 'Berlin', 'DE'
  UNION ALL SELECT 'Warsaw', 'PL'
  UNION ALL SELECT 'Bratislava', 'SK'
  UNION ALL SELECT 'Budapest', 'HU'
  UNION ALL SELECT 'Paris', 'FR'
  UNION ALL SELECT 'Madrid', 'ES'
  UNION ALL SELECT 'Rome', 'IT'
  UNION ALL SELECT 'Amsterdam', 'NL'
  UNION ALL SELECT 'Copenhagen', 'DK'
  UNION ALL SELECT 'Stockholm', 'SE'
  UNION ALL SELECT 'Helsinki', 'FI'
  UNION ALL SELECT 'Lisbon', 'PT'
  UNION ALL SELECT 'Dublin', 'IE'
)
SELECT
  cm.city,
  e.country_code,
  e.year,
  e.gdp_usd,
  e.gdp_growth_pct,
  e.inflation_pct,
  e.unemployment_pct,
  e.population,
  w.avg_temperature_c,
  w.total_precipitation_mm
FROM city_map cm
JOIN silver.economic_indicators e ON cm.country_code = e.country_code
LEFT JOIN silver.weather_daily w ON cm.city = w.city AND e.year = YEAR(w.date)
WHERE e.year = (SELECT MAX(year) FROM silver.economic_indicators);
