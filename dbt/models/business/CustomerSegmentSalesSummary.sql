{{ config(materialized='table') }}

SELECT c.customer_segment,
c.residential_location,
SUM(f.total_amount) AS total_sales,
COUNT(DISTINCT f.sales_id) AS total_transactions,
COUNT(DISTINCT f.customer_sk) AS unique_customers
FROM workspace.gold.FactSales f
JOIN workspace.gold.DimCustomers c ON f.customer_sk = c.customer_sk
GROUP BY c.customer_segment, c.residential_location
ORDER BY total_sales DESC;