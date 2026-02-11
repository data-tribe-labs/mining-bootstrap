-- Gold: Executive summary KPIs (business questions: how is the business doing overall?)
-- Daily production totals and a current-state snapshot for dashboards.

CREATE OR REFRESH MATERIALIZED VIEW gold.executive_summary_daily
COMMENT 'Daily executive KPIs: total production, cost, sites reporting'
AS
SELECT
  production_date AS report_date,
  SUM(tons_extracted) AS total_tons_extracted,
  SUM(total_cost_usd) AS total_production_cost_usd,
  SUM(total_cost_usd) / NULLIF(SUM(tons_extracted), 0) AS blended_cost_per_ton_usd,
  COUNT(DISTINCT site_id) AS sites_reporting_production
FROM silver.daily_production
GROUP BY production_date;


CREATE OR REFRESH MATERIALIZED VIEW gold.fleet_snapshot
COMMENT 'Current fleet and risk snapshot (one row per site for latest state)'
AS
SELECT
  COUNT(DISTINCT s.site_id) AS total_sites,
  SUM(CASE WHEN s.status = 'Active' THEN 1 ELSE 0 END) AS active_sites,
  COUNT(e.equipment_id) AS total_equipment,
  SUM(CASE WHEN e.health_score < 70 OR e.next_maintenance_date <= CURRENT_DATE() THEN 1 ELSE 0 END) AS equipment_at_risk
FROM silver.mine_sites s
LEFT JOIN silver.equipment_maintenance_snapshot e ON e.site_id = s.site_id AND e.__END_AT IS NULL;


CREATE OR REFRESH MATERIALIZED VIEW gold.site_performance_ranked
COMMENT 'Site performance: last 30 days production and cost rank'
AS
SELECT
  s.site_id,
  s.site_name,
  s.primary_mineral,
  s.location,
  SUM(p.tons_extracted) AS tons_last_30d,
  SUM(p.total_cost_usd) AS cost_last_30d_usd,
  SUM(p.total_cost_usd) / NULLIF(SUM(p.tons_extracted), 0) AS cost_per_ton_30d_usd,
  AVG(p.mineral_grade_percent) AS avg_grade_percent_30d
FROM silver.daily_production p
JOIN silver.mine_sites s ON p.site_id = s.site_id
WHERE p.production_date >= CURRENT_DATE() - INTERVAL 30 DAYS
GROUP BY s.site_id, s.site_name, s.primary_mineral, s.location;
