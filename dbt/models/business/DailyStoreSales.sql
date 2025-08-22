{{ config(materialized='table') }}

SELECT s.store_id,
s.store_name,
s.store_location,
d.full_date,
SUM(f.total_amount) AS daily_sales,
COUNT(DISTINCT f.sales_id) AS transactions
FROM workspace.gold.FactSales f
JOIN workspace.gold.DimStores s ON f.store_sk = s.store_sk
JOIN workspace.gold.DimDates d ON f.date_sk = d.date_sk
GROUP BY s.store_id, s.store_name, s.store_location, d.full_date
ORDER BY d.full_date, daily_sales DESC;