# Databricks notebook source

# MAGIC %md
# MAGIC # Gold Strategy 1: Full Overwrite
# MAGIC
# MAGIC Aggregates the **entire** `production_source` table (site × day level) and writes the
# MAGIC result to `production_full_refresh`, **overwriting** it completely on every run.
# MAGIC
# MAGIC ## When to use
# MAGIC - Source data is small-to-medium and a full scan is acceptable.
# MAGIC - Simplicity is paramount — no incremental state to maintain.
# MAGIC - Downstream consumers always need the latest consistent snapshot.
# MAGIC - Deletes in the source **must** be reflected immediately in gold.
# MAGIC
# MAGIC ## Trade-offs
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | ✅ | Simplest implementation; no watermark or state table |
# MAGIC | ✅ | Gold always matches source aggregates exactly — no stale rows |
# MAGIC | ❌ | Full table scan on every run — expensive for very large sources |
# MAGIC | ❌ | No history: the previous gold version is discarded each run |
# MAGIC
# MAGIC ## Data Flow
# MAGIC ```
# MAGIC production_source  (all rows)
# MAGIC        │
# MAGIC        │  groupBy(site_id, production_date)
# MAGIC        │  agg(SUM tons, AVG grade, SUM cost, COUNT)
# MAGIC        │  + _refreshed_at = current_timestamp()
# MAGIC        ▼
# MAGIC production_full_refresh  ← mode("overwrite") each run
# MAGIC ```
# MAGIC
# MAGIC ## Output columns
# MAGIC | Column | Type | Description |
# MAGIC |---|---|---|
# MAGIC | `site_id` | INT | Mine site identifier |
# MAGIC | `production_date` | DATE | Aggregation date |
# MAGIC | `total_tons` | LONG | SUM(tons_extracted) |
# MAGIC | `avg_grade` | DOUBLE | AVG(mineral_grade_percent) |
# MAGIC | `total_cost` | DOUBLE | SUM(total_cost_usd) |
# MAGIC | `record_count` | LONG | COUNT(*) |
# MAGIC | `_refreshed_at` | TIMESTAMP | When this batch ran |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 0: Imports & Parameters
# MAGIC
# MAGIC | Parameter | Default | Description |
# MAGIC |---|---|---|
# MAGIC | `catalog` | `praju_dev` | Unity Catalog name |
# MAGIC | `schema_gold` | `gold_dev` | Gold schema name |

# COMMAND ----------

from pyspark.sql.functions import col, sum as _sum, avg, count, current_timestamp

dbutils.widgets.text("catalog",     "praju_dev")
dbutils.widgets.text("schema_gold", "gold_dev")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Read Source and Aggregate
# MAGIC
# MAGIC Reads all rows from `production_source` and groups by `(site_id, production_date)`.
# MAGIC `_refreshed_at` is stamped with the current timestamp so consumers know when gold was last rebuilt.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Overwrite Target Table
# MAGIC
# MAGIC `mode("overwrite")` with `overwriteSchema=true` replaces the entire table — including any
# MAGIC rows that were deleted from the source since the last run.  This is the key advantage of
# MAGIC the full-refresh strategy over incremental append patterns.

# COMMAND ----------

def run_full_refresh(catalog: str, schema_gold: str):
    source_table = f"{catalog}.{schema_gold}.production_source"
    target_table = f"{catalog}.{schema_gold}.production_full_refresh"

    print(f"Reading source: {source_table}")
    df = spark.table(source_table) \
        .groupBy("site_id", "production_date") \
        .agg(
            _sum("tons_extracted").alias("total_tons"),
            avg("mineral_grade_percent").alias("avg_grade"),
            _sum("total_cost_usd").alias("total_cost"),
            count("*").alias("record_count"),
        ) \
        .withColumn("_refreshed_at", current_timestamp())

    print(f"Writing target (overwrite): {target_table}")
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(target_table)

    row_count = spark.table(target_table).count()
    print(f"✓ {target_table} — {row_count:,} rows written (full overwrite).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Run

# COMMAND ----------

catalog     = dbutils.widgets.get("catalog")
schema_gold = dbutils.widgets.get("schema_gold")

print(f"catalog={catalog}  schema_gold={schema_gold}")
run_full_refresh(catalog, schema_gold)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation
# MAGIC
# MAGIC ```sql
# MAGIC -- 1. Row count and freshness
# MAGIC SELECT COUNT(*) AS row_count, MAX(_refreshed_at) AS last_refresh
# MAGIC FROM   ${catalog}.${schema_gold}.production_full_refresh;
# MAGIC
# MAGIC -- 2. Compare with source aggregate totals (should match exactly)
# MAGIC SELECT
# MAGIC     SUM(total_tons)  AS gold_tons,
# MAGIC     SUM(total_cost)  AS gold_cost
# MAGIC FROM ${catalog}.${schema_gold}.production_full_refresh;
# MAGIC
# MAGIC SELECT
# MAGIC     SUM(tons_extracted) AS source_tons,
# MAGIC     SUM(total_cost_usd) AS source_cost
# MAGIC FROM ${catalog}.${schema_gold}.production_source;
# MAGIC
# MAGIC -- 3. Check no duplicate (site_id, production_date) keys
# MAGIC SELECT site_id, production_date, COUNT(*) AS cnt
# MAGIC FROM   ${catalog}.${schema_gold}.production_full_refresh
# MAGIC GROUP  BY 1, 2
# MAGIC HAVING cnt > 1;
# MAGIC ```
