{{ config(materialized='table') }}

SELECT d.full_date,
d.year,
d.month,
SUM(f.total_amount) AS total_sales,
COUNT(DISTINCT f.sales_id) AS num_transactions
FROM workspace.gold.FactSales f
JOIN workspace.gold.DimDates d ON f.date_sk = d.date_sk
GROUP BY d.full_date, d.year, d.month
ORDER BY d.full_date;