-- Gold: Equipment health and maintenance (business questions: what needs maintenance? where is risk?)
-- Uses current snapshot rows only (__END_AT IS NULL for SCD Type 2 current records).

CREATE OR REFRESH MATERIALIZED VIEW gold.equipment_health_by_site
COMMENT 'Current equipment health and maintenance cost rollup by site'
AS
SELECT
  e.site_id,
  s.site_name,
  s.primary_mineral,
  COUNT(*) AS equipment_count,
  SUM(CASE WHEN e.status = 'Operational' THEN 1 ELSE 0 END) AS operational_count,
  SUM(CASE WHEN e.status = 'Needs Attention' THEN 1 ELSE 0 END) AS needs_attention_count,
  SUM(CASE WHEN e.status = 'Monitor' THEN 1 ELSE 0 END) AS monitor_count,
  AVG(e.health_score) AS avg_health_score,
  SUM(e.annual_maintenance_cost_usd) AS total_annual_maintenance_cost_usd,
  SUM(CASE WHEN e.next_maintenance_date <= CURRENT_DATE() THEN 1 ELSE 0 END) AS maintenance_overdue_count
FROM silver.equipment_maintenance_snapshot e
JOIN silver.mine_sites s ON e.site_id = s.site_id
WHERE e.__END_AT IS NULL
GROUP BY
  e.site_id,
  s.site_name,
  s.primary_mineral;


CREATE OR REFRESH MATERIALIZED VIEW gold.equipment_maintenance_due
COMMENT 'Sites with equipment due for maintenance (overdue or upcoming)'
AS
SELECT
  e.site_id,
  s.site_name,
  s.location,
  e.equipment_id,
  e.equipment_name,
  e.equipment_type,
  e.health_score,
  e.status,
  e.last_maintenance_date,
  e.next_maintenance_date,
  DATEDIFF(e.next_maintenance_date, CURRENT_DATE()) AS days_until_maintenance,
  e.annual_maintenance_cost_usd
FROM silver.equipment_maintenance_snapshot e
JOIN silver.mine_sites s ON e.site_id = s.site_id
WHERE e.__END_AT IS NULL
  AND (e.next_maintenance_date <= CURRENT_DATE() + 30 OR e.health_score < 80);
