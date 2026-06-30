MODEL (
  name gold.sales_analytics,
  kind VIEW,
  owner data,
  description 'Sales analytics: revenue by city, category, and top products'
);

WITH revenue_by_city AS (
  SELECT
    city,
    country_code,
    SUM(revenue_eur) AS total_revenue_eur,
    COUNT(DISTINCT sale_date) AS active_days,
    ROUND(SUM(revenue_eur) / NULLIF(COUNT(DISTINCT sale_date), 0), 2) AS daily_avg_revenue_eur
  FROM silver.sales
  GROUP BY city, country_code
),
revenue_by_category AS (
  SELECT
    city,
    product_category,
    SUM(revenue_eur) AS category_revenue_eur,
    ROUND(
      SUM(revenue_eur) * 100.0 / NULLIF(SUM(SUM(revenue_eur)) OVER (PARTITION BY city), 0),
      1
    ) AS category_share_pct
  FROM silver.sales
  GROUP BY city, product_category
),
top_products AS (
  SELECT
    city,
    product_name,
    SUM(revenue_eur) AS product_revenue_eur,
    ROW_NUMBER() OVER (PARTITION BY city ORDER BY SUM(revenue_eur) DESC) AS rank
  FROM silver.sales
  GROUP BY city, product_name
  QUALIFY rank <= 5
)
SELECT
  rbc.city,
  rbc.country_code,
  rbc.total_revenue_eur,
  rbc.active_days,
  rbc.daily_avg_revenue_eur,
  rbc2.product_category,
  rbc2.category_revenue_eur,
  rbc2.category_share_pct,
  tp.product_name AS top_product,
  tp.product_revenue_eur AS top_product_revenue_eur
FROM revenue_by_city rbc
LEFT JOIN revenue_by_category rbc2 ON rbc.city = rbc2.city
LEFT JOIN top_products tp ON rbc.city = tp.city
ORDER BY rbc.total_revenue_eur DESC, rbc2.category_revenue_eur DESC;
