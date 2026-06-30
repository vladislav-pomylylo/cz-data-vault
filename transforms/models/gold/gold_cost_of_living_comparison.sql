MODEL (
  name gold.cost_of_living_comparison,
  kind VIEW,
  owner data,
  description 'GDP per capita, inflation and unemployment by country (2020+)'
);

SELECT
  country_code,
  year,
  gdp_usd / NULLIF(population, 0) AS gdp_per_capita,
  inflation_pct,
  unemployment_pct
FROM silver.economic_indicators
WHERE year >= 2020;
