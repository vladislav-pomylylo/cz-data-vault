MODEL (
  name silver.sales,
  kind VIEW,
  owner data,
  description 'Sales data with EUR conversion and category normalisation'
);

WITH rates AS (
  SELECT 'EUR' AS currency, 1.0 AS rate_to_eur
  UNION ALL SELECT 'CZK', 0.04
  UNION ALL SELECT 'PLN', 0.23
  UNION ALL SELECT 'HUF', 0.0026
)
SELECT
  s.sale_date,
  s.country_code,
  s.city,
  COALESCE(s.product_category, 'Unknown') AS product_category,
  s.product_name,
  s.quantity,
  s.unit_price,
  s.currency,
  s.quantity * s.unit_price * COALESCE(r.rate_to_eur, 0) AS revenue_eur,
  s.quantity * s.unit_price AS revenue_local,
  s._filename,
  s._ingested_at
FROM bronze.sales s
LEFT JOIN rates r ON s.currency = r.currency
WHERE s.quantity > 0
  AND s.unit_price > 0;
