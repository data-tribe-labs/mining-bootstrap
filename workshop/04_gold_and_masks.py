# Databricks notebook source
# MAGIC %md
# MAGIC # Module 4 — Gold tables + Column/Row masks
# MAGIC
# MAGIC Two business-ready gold tables, each protected with a different UC governance primitive:
# MAGIC
# MAGIC | Gold table | Protection |
# MAGIC |---|---|
# MAGIC | `gold.daily_production_summary` | **Column mask** on `total_cost` — non-finance users see it rounded to the nearest $1,000 |
# MAGIC | `gold.equipment_risk_summary` | **Row filter** — non-ops users only see rows for mines with status = `Active` |
# MAGIC
# MAGIC Both are implemented as UC SQL **functions** applied to the table with
# MAGIC `ALTER TABLE ... SET [COLUMN MASK | ROW FILTER]`. The function decides what the
# MAGIC querying user is allowed to see based on `is_account_group_member(...)`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Config — edit this cell

# COMMAND ----------

# EDIT THIS — point to your own catalog
CATALOG = "workshop_firstname_lastname"

SCHEMA_SILVER = "silver"
SCHEMA_GOLD   = "gold"

# Account groups used by the masks. The masks will behave as if you are NOT in these
# groups unless you actually are — no harm either way for a demo.
FINANCE_GROUP = "finance"
OPS_GROUP     = "mining_ops"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Build `gold.daily_production_summary`
# MAGIC
# MAGIC Aggregate silver production to site × day. Simple `groupBy` + `agg`, full overwrite.

# COMMAND ----------

def build_daily_production_summary() -> None:
    target = f"{CATALOG}.{SCHEMA_GOLD}.daily_production_summary"

    spark.sql(f"""
        CREATE OR REPLACE TABLE {target}
        USING DELTA AS
        SELECT
            site_id,
            production_date,
            SUM(tons_extracted)        AS total_tons,
            AVG(mineral_grade_percent) AS avg_grade,
            SUM(total_cost_usd)        AS total_cost,
            COUNT(*)                   AS record_count,
            CURRENT_TIMESTAMP()        AS _refreshed_at
        FROM {CATALOG}.{SCHEMA_SILVER}.daily_production
        GROUP BY site_id, production_date
    """)

    print(f"✓ {target}  —  {spark.table(target).count():,} rows")

build_daily_production_summary()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Build `gold.equipment_risk_summary`
# MAGIC
# MAGIC Join silver equipment with silver mine_sites (via bronze since we didn't silver the
# MAGIC sites table) so we have `site_status` alongside each piece of equipment.

# COMMAND ----------

def build_equipment_risk_summary() -> None:
    target = f"{CATALOG}.{SCHEMA_GOLD}.equipment_risk_summary"

    spark.sql(f"""
        CREATE OR REPLACE TABLE {target}
        USING DELTA AS
        SELECT
            e.equipment_id,
            e.equipment_name,
            e.equipment_type,
            e.site_id,
            s.site_name,
            s.location,
            s.status AS site_status,
            e.health_score,
            e.status,
            e.operating_hours,
            e.next_maintenance_date,
            e.annual_maintenance_cost_usd,
            CURRENT_TIMESTAMP() AS _refreshed_at
        FROM {CATALOG}.{SCHEMA_SILVER}.equipment_maintenance_snapshot e
        LEFT JOIN {CATALOG}.bronze.mine_sites s
            ON e.site_id = s.site_id
    """)

    print(f"✓ {target}  —  {spark.table(target).count():,} rows")

build_equipment_risk_summary()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Column mask on `total_cost`
# MAGIC
# MAGIC Create a SQL function that takes the real cost and returns either:
# MAGIC - the full precision value (for users in `finance`), or
# MAGIC - the value rounded to the nearest $1,000 (everyone else)
# MAGIC
# MAGIC Then attach it to the column with `ALTER TABLE ... ALTER COLUMN ... SET MASK`.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA_GOLD}.mask_total_cost(cost DOUBLE)
RETURNS DOUBLE
RETURN
    CASE
        WHEN is_account_group_member('{FINANCE_GROUP}') THEN cost
        ELSE ROUND(cost / 1000) * 1000
    END
""")

spark.sql(f"""
ALTER TABLE {CATALOG}.{SCHEMA_GOLD}.daily_production_summary
ALTER COLUMN total_cost
SET MASK {CATALOG}.{SCHEMA_GOLD}.mask_total_cost
""")

print("Column mask attached.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Row filter on `equipment_risk_summary`
# MAGIC
# MAGIC Users outside the `mining_ops` group only see equipment at **Active** sites. Under-
# MAGIC maintenance sites are hidden from the general audience.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA_GOLD}.filter_by_site_status(site_status STRING)
RETURNS BOOLEAN
RETURN
    is_account_group_member('{OPS_GROUP}') OR site_status = 'Active'
""")

spark.sql(f"""
ALTER TABLE {CATALOG}.{SCHEMA_GOLD}.equipment_risk_summary
SET ROW FILTER {CATALOG}.{SCHEMA_GOLD}.filter_by_site_status ON (site_status)
""")

print("Row filter attached.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: See the masks in action
# MAGIC
# MAGIC Querying as you (not in either group) — the numbers for `total_cost` will look
# MAGIC suspiciously round, and you'll see fewer rows in `equipment_risk_summary` than the
# MAGIC raw count.

# COMMAND ----------

display(spark.sql(f"""
    SELECT site_id, production_date, total_tons, total_cost, _refreshed_at
    FROM {CATALOG}.{SCHEMA_GOLD}.daily_production_summary
    ORDER BY production_date DESC, site_id
    LIMIT 10
"""))

# COMMAND ----------

display(spark.sql(f"""
    SELECT equipment_id, equipment_name, site_id, site_name, site_status, health_score, status
    FROM {CATALOG}.{SCHEMA_GOLD}.equipment_risk_summary
    ORDER BY health_score
    LIMIT 15
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Have a neighbour query it
# MAGIC
# MAGIC Ask a neighbour to run the same `SELECT` against your catalog (they have read access):
# MAGIC
# MAGIC ```sql
# MAGIC SELECT * FROM <your_catalog>.gold.daily_production_summary LIMIT 5;
# MAGIC SELECT COUNT(*) FROM <your_catalog>.gold.equipment_risk_summary;
# MAGIC ```
# MAGIC
# MAGIC They will see the same masked values you see, because they are also not in the
# MAGIC `finance` or `mining_ops` groups. The beauty: one mask definition, enforced for
# MAGIC every viewer automatically.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Recap
# MAGIC
# MAGIC You now have:
# MAGIC - `gold.daily_production_summary` with a column mask (group-aware)
# MAGIC - `gold.equipment_risk_summary` with a row filter (group-aware)
# MAGIC
# MAGIC Masks + filters are **attached to the table**, so any downstream consumer — SQL
# MAGIC warehouse, Genie, dashboards, notebooks — gets the governed view automatically.
# MAGIC
# MAGIC Next: Module 5 — lineage, time travel, and orchestration.
