MODEL (
  name gold.population_growth_trends,
  kind VIEW,
  owner data,
  description 'Year-over-year population growth trends by country'
);

SELECT
  country_code,
  year,
  population,
  LAG(population) OVER (PARTITION BY country_code ORDER BY year) AS prev_year_population,
  ROUND(
    (population - LAG(population) OVER (PARTITION BY country_code ORDER BY year))
    / NULLIF(LAG(population) OVER (PARTITION BY country_code ORDER BY year), 0) * 100,
    2
  ) AS growth_pct
FROM silver.economic_indicators
WHERE population IS NOT NULL
ORDER BY country_code, year;
