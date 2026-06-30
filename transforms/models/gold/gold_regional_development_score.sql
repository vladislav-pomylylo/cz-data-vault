MODEL (
  name gold.regional_development_score,
  kind VIEW,
  owner data,
  description 'Composite development score based on GDP per capita, inflation and unemployment'
);

SELECT
  country_code,
  year,
  gdp_per_capita,
  inflation_pct,
  unemployment_pct,
  ROUND(
    (gdp_per_capita / NULLIF(MAX(gdp_per_capita) OVER (), 0)) * 50
    + GREATEST(100 - inflation_pct, 0) * 0.25
    + GREATEST(100 - unemployment_pct, 0) * 0.25,
    2
  ) AS development_score
FROM gold.cost_of_living_comparison;
