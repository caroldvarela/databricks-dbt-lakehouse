{{ config(materialized='table') }}

SELECT sp.salesperson_id,
sp.salesperson_name,
sp.salesperson_role,
SUM(f.total_amount) AS total_sales,
COUNT(DISTINCT f.sales_id) AS total_transactions
FROM workspace.gold.FactSales f
JOIN workspace.gold.DimSalesPersons sp ON f.salesperson_sk = sp.salesperson_sk
GROUP BY sp.salesperson_id, sp.salesperson_name, sp.salesperson_role
ORDER BY total_sales DESC;