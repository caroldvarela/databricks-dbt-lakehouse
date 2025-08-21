{{ config(materialized='table') }}

SELECT p.category,
d.year,
DATE_FORMAT(MAKE_DATE(d.year, d.month, 1), 'MMMM') AS month_name,
SUM(f.total_amount) AS total_sales,
COUNT(DISTINCT f.sales_id) AS transactions
FROM workspace.gold.FactSales f
JOIN workspace.gold.DimProducts p ON f.product_sk = p.product_sk
JOIN workspace.gold.DimDates d ON f.date_sk = d.date_sk
GROUP BY p.category, d.year, d.month
ORDER BY d.year, d.month, total_sales DESC;