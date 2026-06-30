MODEL (
  name bronze.worldbank,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column year,
    batch_size 10
  ),
  audits (
    not_null(columns := (country_code, year, indicator_name, value)),
    unique_combination(columns := (country_code, year, indicator))
  ),
  owner data,
  cron '@monthly',
  description 'World Bank economic indicators: GDP, inflation, unemployment, population'
);

SELECT
  country_code,
  year,
  indicator,
  indicator_name,
  value,
  ingestion_dt AS _ingested_at
FROM
  raw.worldbank
WHERE
  year BETWEEN @start_date AND @end_date;
