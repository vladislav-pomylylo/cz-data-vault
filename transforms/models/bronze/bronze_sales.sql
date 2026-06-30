MODEL (
  name bronze.sales,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column sale_date,
    batch_size 7
  ),
  audits (
    not_null(columns := (sale_date, country_code, city, product_name, quantity, unit_price)),
    unique_combination(columns := (sale_date, city, product_name, _filename))
  ),
  owner data,
  cron '@daily',
  description 'Raw sales data from manually dropped CSV files'
);

SELECT
  sale_date,
  country_code,
  city,
  product_category,
  product_name,
  quantity,
  unit_price,
  currency,
  _filename,
  _ingested_at
FROM
  raw.csv_sales
WHERE
  sale_date BETWEEN @start_date AND @end_date;
