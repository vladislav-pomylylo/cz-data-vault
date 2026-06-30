MODEL (
  name silver.economic_indicators,
  kind VIEW,
  owner data,
  description 'Economic indicators pivoted from EAV to wide format'
);

SELECT
  country_code,
  year,
  MAX(CASE WHEN indicator_name = 'gdp_usd' THEN value END) AS gdp_usd,
  MAX(CASE WHEN indicator_name = 'gdp_growth_pct' THEN value END) AS gdp_growth_pct,
  MAX(CASE WHEN indicator_name = 'inflation_pct' THEN value END) AS inflation_pct,
  MAX(CASE WHEN indicator_name = 'unemployment_pct' THEN value END) AS unemployment_pct,
  MAX(CASE WHEN indicator_name = 'population' THEN value END) AS population
FROM bronze.worldbank
GROUP BY country_code, year;
