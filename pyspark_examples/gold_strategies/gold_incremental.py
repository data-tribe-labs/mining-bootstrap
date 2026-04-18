# Databricks notebook source

# MAGIC %md
# MAGIC # Gold Strategy 2: Watermark-based Incremental Append
# MAGIC
# MAGIC Reads only records from `production_source` whose `production_datetime` is **strictly
# MAGIC greater than the watermark** stored in the gold table itself, then appends the
# MAGIC aggregated result as a new batch.
# MAGIC
# MAGIC ## When to use
# MAGIC - Source is **append-only** or inserts dominate (no deletes, few updates).
# MAGIC - The ordering column (`production_datetime`) is monotonically non-decreasing for new data.
# MAGIC - You want a lightweight pattern with **no external state store**.
# MAGIC
# MAGIC ## Trade-offs
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | ✅ | Only scans new data — very efficient for large historical tables |
# MAGIC | ✅ | No external state store — watermark lives in the gold table itself |
# MAGIC | ❌ | Does NOT reflect source deletes or updates of past records |
# MAGIC | ❌ | Late-arriving data (production_datetime < watermark) is silently skipped |
# MAGIC
# MAGIC ## Watermark Flow
# MAGIC ```
# MAGIC Run N-1:  watermark_start = "1970-01-01"  (first run, loads full history)
# MAGIC           gold table written with MAX(_watermark_start) = "1970-01-01"
# MAGIC
# MAGIC Run N:    watermark = MAX(_watermark_start) from gold = "1970-01-01"
# MAGIC           filter source: production_datetime > "1970-01-01" + new inserts only
# MAGIC           append batch; gold table now has two batches
# MAGIC
# MAGIC Run N+1:  watermark = MAX(_watermark_start) from gold = "1970-01-01"
# MAGIC           (no new inserts → exits early)
# MAGIC
# MAGIC After simulate_changes.py adds 3 days of rows:
# MAGIC Run N+2:  watermark still "1970-01-01" → picks up the 3 600 new rows
# MAGIC           appends batch with _watermark_start = "1970-01-01"
# MAGIC ```
# MAGIC
# MAGIC > **Note:** The watermark is the `_watermark_start` of the *previous* batch, not
# MAGIC > the max `production_datetime` of that batch.  This ensures idempotent re-runs.
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
# MAGIC | `_loaded_at` | TIMESTAMP | When this batch ran |
# MAGIC | `_watermark_start` | TIMESTAMP | Lower bound used for this batch |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 0: Imports & Parameters
# MAGIC
# MAGIC | Parameter | Default | Description |
# MAGIC |---|---|---|
# MAGIC | `catalog` | `praju_dev` | Unity Catalog name |
# MAGIC | `schema_gold` | `gold_dev` | Gold schema name |

# COMMAND ----------

from pyspark.sql.functions import col, sum as _sum, avg, count, current_timestamp, lit

dbutils.widgets.text("catalog",     "praju_dev")
dbutils.widgets.text("schema_gold", "gold_dev")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Get Watermark
# MAGIC
# MAGIC On **first run** the target table does not exist → watermark defaults to `1970-01-01`
# MAGIC so the full source history is loaded.
# MAGIC
# MAGIC On **subsequent runs** the watermark is `MAX(_watermark_start)` from the gold table —
# MAGIC the lower bound of the last successfully appended batch.

# COMMAND ----------

def get_watermark(target_table: str) -> str:
    """Return the last watermark stored in the gold table (epoch if first run)."""
    if not spark.catalog.tableExists(target_table):
        return "1970-01-01"
    row = spark.sql(
        f"SELECT COALESCE(MAX(_watermark_start), '1970-01-01') AS wm FROM {target_table}"
    ).collect()[0]
    return str(row["wm"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Filter New Rows and Aggregate
# MAGIC
# MAGIC Filters `production_source` to only rows with `production_datetime > watermark`.
# MAGIC If there are no new rows the function exits early without writing.
# MAGIC
# MAGIC The aggregate is grouped by `(site_id, production_date)` and stamped with:
# MAGIC - `_loaded_at` — the current timestamp (when this batch ran)
# MAGIC - `_watermark_start` — the watermark used for this batch (for the next run to read)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Append to Gold Table
# MAGIC
# MAGIC `mode("append")` means the gold table grows monotonically — one batch per run.
# MAGIC Use `_watermark_start` to identify which batch each row belongs to.

# COMMAND ----------

def run_incremental(catalog: str, schema_gold: str):
    source_table = f"{catalog}.{schema_gold}.production_source"
    target_table = f"{catalog}.{schema_gold}.production_incremental"

    watermark = get_watermark(target_table)
    print(f"Watermark: {watermark}")

    df_new = spark.table(source_table) \
        .filter(col("production_datetime") > lit(watermark))

    new_source_count = df_new.count()
    print(f"New source rows since watermark: {new_source_count:,}")

    if new_source_count == 0:
        print("No new records to process. Exiting.")
        return

    df_gold = df_new \
        .groupBy("site_id", "production_date") \
        .agg(
            _sum("tons_extracted").alias("total_tons"),
            avg("mineral_grade_percent").alias("avg_grade"),
            _sum("total_cost_usd").alias("total_cost"),
            count("*").alias("record_count"),
        ) \
        .withColumn("_loaded_at",       current_timestamp()) \
        .withColumn("_watermark_start", lit(watermark).cast("timestamp"))

    print(f"Appending to: {target_table}")
    df_gold.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(target_table)

    total_rows = spark.table(target_table).count()
    print(f"✓ {target_table} — {total_rows:,} total rows (appended this batch).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Run

# COMMAND ----------

catalog     = dbutils.widgets.get("catalog")
schema_gold = dbutils.widgets.get("schema_gold")

print(f"catalog={catalog}  schema_gold={schema_gold}")
run_incremental(catalog, schema_gold)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation
# MAGIC
# MAGIC ```sql
# MAGIC -- 1. Inspect watermark batches
# MAGIC SELECT _watermark_start, COUNT(*) AS rows, MAX(_loaded_at) AS loaded_at
# MAGIC FROM   ${catalog}.${schema_gold}.production_incremental
# MAGIC GROUP  BY 1
# MAGIC ORDER  BY 1;
# MAGIC
# MAGIC -- 2. No duplicates within a single watermark window
# MAGIC SELECT site_id, production_date, _watermark_start, COUNT(*) AS cnt
# MAGIC FROM   ${catalog}.${schema_gold}.production_incremental
# MAGIC GROUP  BY 1, 2, 3
# MAGIC HAVING cnt > 1
# MAGIC ORDER  BY cnt DESC
# MAGIC LIMIT  5;
# MAGIC
# MAGIC -- 3. Current watermark value
# MAGIC SELECT MAX(_watermark_start) AS current_watermark
# MAGIC FROM   ${catalog}.${schema_gold}.production_incremental;
# MAGIC ```
