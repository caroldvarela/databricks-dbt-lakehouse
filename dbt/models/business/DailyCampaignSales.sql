{{ config(materialized='table') }}

SELECT c.campaign_id,
c.campaign_name,
d.full_date,
SUM(f.total_amount) AS daily_sales,
COUNT(DISTINCT f.sales_id) AS transactions,
COUNT(DISTINCT f.customer_sk) AS unique_customers
FROM workspace.gold.FactSales f
JOIN workspace.gold.DimCampaigns c
ON f.campaign_sk = c.campaign_sk
JOIN workspace.gold.DimDates d ON f.date_sk = d.date_sk
GROUP BY c.campaign_id, c.campaign_name, d.full_date
ORDER BY c.campaign_id, d.full_date;