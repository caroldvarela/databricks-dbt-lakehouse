{{ config(materialized='table') }}

SELECT sp.salesperson_id,
sp.salesperson_name,
sp.salesperson_role,
d.year,
d.month,
SUM(f.total_amount) AS total_sales,
COUNT(DISTINCT f.sales_id) AS transactions
FROM workspace.gold.FactSales f
JOIN workspace.gold.DimSalesPersons sp ON f.salesperson_sk = sp.salesperson_sk
JOIN workspace.gold.DimDates d ON f.date_sk = d.date_sk
GROUP BY sp.salesperson_id, sp.salesperson_name, sp.salesperson_role, d.year, d.month
ORDER BY d.year, d.month, total_sales DESC;