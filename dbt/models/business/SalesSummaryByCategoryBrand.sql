{{ config(materialized='table') }}

SELECT 
p.category,
p.brand,
SUM(f.total_amount) AS total_sales,
COUNT(DISTINCT f.sales_id) AS num_transactions
FROM workspace.gold.FactSales f
JOIN workspace.gold.DimProducts p ON f.product_sk = p.product_sk
GROUP BY p.category, p.brand
ORDER BY total_sales DESC;

