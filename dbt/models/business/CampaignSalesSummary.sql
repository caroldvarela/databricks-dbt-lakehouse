{{ config(materialized='table') }}

SELECT c.campaign_id,
c.campaign_name,
d_start.full_date AS start_date,
d_end.full_date AS end_date,
SUM(f.total_amount) AS total_sales,
COUNT(DISTINCT f.sales_id) AS total_transactions,
COUNT(DISTINCT f.customer_sk) AS unique_customers
FROM workspace.gold.FactSales f
JOIN workspace.gold.DimCampaigns c ON f.campaign_sk = c.campaign_sk
LEFT JOIN workspace.gold.DimDates d_start ON c.start_date_sk = d_start.date_sk
LEFT JOIN workspace.gold.DimDates d_end ON c.end_date_sk = d_end.date_sk
GROUP BY c.campaign_id, c.campaign_name, d_start.full_date, d_end.full_date
ORDER BY total_sales DESC;