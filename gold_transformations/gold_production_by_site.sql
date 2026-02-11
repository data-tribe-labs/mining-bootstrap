-- Gold: Production by site and by mineral (business questions: where do we produce? which minerals?)
-- Joins production with mine sites for site/mineral attributes.

CREATE OR REFRESH MATERIALIZED VIEW gold.production_by_site
COMMENT 'Daily production rollup by site with site and mineral attributes'
AS
SELECT
  s.site_id,
  s.site_name,
  s.location,
  s.primary_mineral,
  s.mining_method,
  s.status AS site_status,
  p.production_date,
  SUM(p.tons_extracted) AS tons_extracted,
  AVG(p.mineral_grade_percent) AS avg_grade_percent,
  SUM(p.total_cost_usd) AS total_cost_usd,
  SUM(p.operating_hours) AS operating_hours,
  COUNT(1) AS record_count
FROM silver.daily_production p
JOIN silver.mine_sites s ON p.site_id = s.site_id
GROUP BY
  s.site_id,
  s.site_name,
  s.location,
  s.primary_mineral,
  s.mining_method,
  s.status,
  p.production_date;


CREATE OR REFRESH MATERIALIZED VIEW gold.production_by_mineral
COMMENT 'Production rollup by primary mineral (portfolio view)'
AS
SELECT
  s.primary_mineral,
  p.production_date,
  COUNT(DISTINCT s.site_id) AS site_count,
  SUM(p.tons_extracted) AS total_tons_extracted,
  AVG(p.mineral_grade_percent) AS avg_grade_percent,
  SUM(p.total_cost_usd) AS total_cost_usd,
  SUM(p.total_cost_usd) / NULLIF(SUM(p.tons_extracted), 0) AS avg_cost_per_ton_usd
FROM silver.daily_production p
JOIN silver.mine_sites s ON p.site_id = s.site_id
GROUP BY
  s.primary_mineral,
  p.production_date;
