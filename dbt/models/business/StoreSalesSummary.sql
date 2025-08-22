{{ config(materialized='table') }}

SELECT s.store_id,
s.store_name,
s.store_type,
s.store_location,
SUM(f.total_amount) AS total_sales,
COUNT(DISTINCT f.sales_id) AS total_transactions
FROM workspace.gold.FactSales f
JOIN workspace.gold.DimStores s ON f.store_sk = s.store_sk
GROUP BY s.store_id, s.store_name, s.store_type, s.store_location
ORDER BY total_sales DESC;