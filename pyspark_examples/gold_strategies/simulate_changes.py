# Databricks notebook source

# MAGIC %md
# MAGIC # Gold Strategies — Step 2: Simulate Changes (DELETEs, UPDATEs, INSERTs)
# MAGIC
# MAGIC This script applies realistic changes to `production_source` so that the four
# MAGIC gold load strategies have non-trivial CDF history to process.
# MAGIC
# MAGIC **Run this between gold strategy runs** to simulate a new batch of source changes.
# MAGIC
# MAGIC ## Change Summary
# MAGIC
# MAGIC ```
# MAGIC production_source (before)
# MAGIC        │
# MAGIC        ├── DELETE  ~1 % of last-30-day records  (hard deletes)
# MAGIC        ├── UPDATE  ~2 % of all records          (tons_extracted, total_cost_usd)
# MAGIC        └── INSERT  3 days × 50 sites × 24 hrs  = 3 600 new rows
# MAGIC        │
# MAGIC production_source (after)  → CDF now has delete / update_preimage /
# MAGIC                               update_postimage / insert records
# MAGIC ```
# MAGIC
# MAGIC Each operation is recorded in the Delta transaction log, which allows the CDF-based
# MAGIC gold strategies (`gold_cdc.py`, `gold_no_delete.py`) to replay only the changes
# MAGIC since their last run.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 0: Imports & Parameters
# MAGIC
# MAGIC | Parameter | Default | Description |
# MAGIC |---|---|---|
# MAGIC | `catalog` | `praju_dev` | Unity Catalog name |
# MAGIC | `schema_gold` | `gold_dev` | Gold schema name |

# COMMAND ----------

from pyspark.sql.functions import col, rand, lit, current_timestamp, floor

dbutils.widgets.text("catalog",     "praju_dev")
dbutils.widgets.text("schema_gold", "gold_dev")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Helper — Table Name Builder

# COMMAND ----------

def _table(catalog: str, schema: str, name: str) -> str:
    return f"{catalog}.{schema}.{name}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Apply DELETEs (~1 % of last-30-day records)
# MAGIC
# MAGIC Samples a random 1 % of rows where `production_datetime >= current_date - 30 days`
# MAGIC and issues a correlated `DELETE` against the source table.
# MAGIC
# MAGIC These rows will appear as `_change_type = 'delete'` in the CDF stream, allowing
# MAGIC `gold_cdc.py` to propagate the hard-delete to the gold table.
# MAGIC `gold_no_delete.py` intentionally ignores them.

# COMMAND ----------

def apply_deletes(source_table: str, sample_fraction: float = 0.01):
    """Delete ~1 % of records from the last 30 days."""
    print("Applying DELETEs (~1 % of last-30-day records)…")

    df_recent = spark.sql(f"""
        SELECT site_id, production_datetime
        FROM   {source_table}
        WHERE  production_datetime >= current_date() - INTERVAL 30 DAYS
    """)

    df_sample = df_recent.sample(withReplacement=False, fraction=sample_fraction, seed=42)
    df_sample.createOrReplaceTempView("_deletes_vw")

    spark.sql(f"""
        DELETE FROM {source_table} t
        WHERE EXISTS (
            SELECT 1 FROM _deletes_vw s
            WHERE s.site_id = t.site_id AND s.production_datetime = t.production_datetime
        )
    """)

    deleted_count = df_sample.count()
    print(f"  ✓ Deleted {deleted_count:,} rows.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Apply UPDATEs (~2 % of all records)
# MAGIC
# MAGIC Randomly samples 2 % of all rows and adjusts `tons_extracted` and
# MAGIC `total_cost_usd` by a factor in `[0.9, 1.1]`.
# MAGIC
# MAGIC Each updated row generates **two** CDF records:
# MAGIC - `update_preimage` — the value before the change
# MAGIC - `update_postimage` — the value after the change
# MAGIC
# MAGIC Gold strategies that use CDF only keep `update_postimage` (the latest value).

# COMMAND ----------

def apply_updates(source_table: str, sample_fraction: float = 0.02):
    """Update tons_extracted and total_cost_usd for ~2 % of all records."""
    print("Applying UPDATEs (~2 % of all records)…")

    df_sample = spark.table(source_table) \
        .sample(withReplacement=False, fraction=sample_fraction, seed=99) \
        .select("site_id", "production_datetime") \
        .withColumn("adjustment", lit(0.9) + rand(seed=99) * lit(0.2))

    df_sample.createOrReplaceTempView("_updates_vw")

    spark.sql(f"""
        MERGE INTO {source_table} AS t
        USING _updates_vw                            AS s
        ON    t.site_id              = s.site_id
          AND t.production_datetime  = s.production_datetime
        WHEN MATCHED THEN UPDATE SET
            t.tons_extracted  = CAST(t.tons_extracted * s.adjustment AS INT),
            t.total_cost_usd  = t.tons_extracted * t.cost_per_ton_usd
    """)

    updated_count = df_sample.count()
    print(f"  ✓ Updated {updated_count:,} rows.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Apply INSERTs (3 days × 50 sites × 24 hours = 3 600 rows)
# MAGIC
# MAGIC Generates new rows for the **3 days after the current `MAX(production_datetime)`**
# MAGIC in the source table, covering all 50 sites and all 24 hours per day.
# MAGIC
# MAGIC These rows will be picked up by `gold_incremental.py` (watermark-based) and
# MAGIC by the CDF-based strategies as `_change_type = 'insert'`.
# MAGIC
# MAGIC Schema is built to match the target exactly using `target_schema` introspection,
# MAGIC avoiding type-mismatch errors during the append write.

# COMMAND ----------

def apply_inserts(source_table: str, catalog: str, schema: str):
    """
    Insert 3 future days × 50 sites × 24 hours = 3 600 new rows.

    The new rows use production_datetime values one day beyond the current
    MAX in the table so the incremental watermark strategy will pick them up.
    """
    from pyspark.sql.functions import date_add, make_interval, col as _col
    from pyspark.sql import functions as F
    from datetime import timedelta

    print("Applying INSERTs (3 days × 50 sites × 24 hours = 3 600 rows)…")

    # Determine the next day after the current max datetime
    max_dt = spark.sql(f"SELECT MAX(production_datetime) FROM {source_table}").collect()[0][0]
    print(f"  Current MAX production_datetime: {max_dt}")

    # Read the exact target schema so we can cast new rows to match precisely.
    target_schema = spark.table(source_table).schema
    type_map = {f.name: f.dataType for f in target_schema}

    # Generate rows in Python — 3 days × 50 sites × 24 hours = 3600 rows.
    # This avoids any SQL type-inference surprises for a small batch.
    from datetime import datetime, timedelta
    import math

    rows = []
    base_dt = max_dt if isinstance(max_dt, datetime) else datetime.fromisoformat(str(max_dt))
    for day_offset in range(1, 4):       # 3 future days
        for site_id in range(1, 51):     # 50 sites
            for hour in range(0, 24):    # 24 hours
                prod_dt   = base_dt.replace(hour=0, minute=0, second=0, microsecond=0) \
                            + timedelta(days=day_offset, hours=hour)
                prod_date = prod_dt.date()
                tons      = 50 + (site_id * 7 + day_offset * 13 + hour * 3) % 100
                grade     = 1.0 + (site_id + day_offset + hour) % 5
                cost_pt   = 25.0 + (site_id + hour) % 30
                rows.append((site_id, prod_dt, prod_date, tons, grade, 1.0, cost_pt, float(tons * cost_pt)))

    from pyspark.sql.types import StructType, StructField, IntegerType, TimestampType, DateType, DoubleType
    schema = StructType([
        StructField("site_id",               IntegerType(),   False),
        StructField("production_datetime",   TimestampType(), False),
        StructField("production_date",       DateType(),      False),
        StructField("tons_extracted",        IntegerType(),   False),
        StructField("mineral_grade_percent", DoubleType(),    False),
        StructField("operating_hours",       DoubleType(),    False),
        StructField("cost_per_ton_usd",      DoubleType(),    False),
        StructField("total_cost_usd",        DoubleType(),    False),
    ])

    # Cast each column to the exact type in the target table to avoid merge errors.
    new_rows = spark.createDataFrame(rows, schema=schema)
    for field in target_schema:
        if field.name in [f.name for f in schema.fields]:
            new_rows = new_rows.withColumn(field.name, _col(field.name).cast(field.dataType))

    new_rows.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(source_table)

    print(f"  ✓ Inserted {len(rows):,} new rows.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Print CDF Summary
# MAGIC
# MAGIC After all three change operations, this reads the Delta history and the CDF stream
# MAGIC to show a breakdown of change types applied in this run.
# MAGIC
# MAGIC **CDF change types you will see:**
# MAGIC
# MAGIC | `_change_type` | Produced by |
# MAGIC |---|---|
# MAGIC | `insert` | `apply_inserts` |
# MAGIC | `update_preimage` | `apply_updates` (before value) |
# MAGIC | `update_postimage` | `apply_updates` (after value) |
# MAGIC | `delete` | `apply_deletes` |

# COMMAND ----------

def print_cdf_summary(source_table: str):
    """Print the last 5 history entries and a CDF change-type breakdown."""
    print("\n--- DESCRIBE HISTORY (last 5 operations) ---")
    spark.sql(f"DESCRIBE HISTORY {source_table} LIMIT 5").show(truncate=False)

    # Get the two most recent versions to read CDF for the just-applied changes
    versions = spark.sql(
        f"SELECT version FROM (DESCRIBE HISTORY {source_table} LIMIT 10) ORDER BY version DESC LIMIT 4"
    ).collect()

    if len(versions) >= 2:
        start_v = versions[-1]["version"]
        end_v   = versions[0]["version"]
        print(f"\n--- CDF summary (versions {start_v} → {end_v}) ---")
        # Use SQL table_changes() to avoid Spark Connect cross-DataFrame ref issues.
        spark.sql(f"""
            SELECT _change_type, COUNT(*) AS count
            FROM   table_changes('{source_table}', {start_v}, {end_v})
            GROUP  BY _change_type
            ORDER  BY _change_type
        """).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Run

# COMMAND ----------

catalog     = dbutils.widgets.get("catalog")
schema_gold = dbutils.widgets.get("schema_gold")

print(f"catalog={catalog}  schema_gold={schema_gold}")
source_table = _table(catalog, schema_gold, "production_source")

apply_deletes(source_table)
apply_updates(source_table)
apply_inserts(source_table, catalog, schema_gold)
print_cdf_summary(source_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation
# MAGIC
# MAGIC Run these after the script completes to inspect the CDF history.
# MAGIC
# MAGIC ```sql
# MAGIC -- 1. Full Delta history
# MAGIC DESCRIBE HISTORY ${catalog}.${schema_gold}.production_source;
# MAGIC
# MAGIC -- 2. CDF breakdown for the last 4 versions
# MAGIC SELECT _change_type, COUNT(*) AS cnt
# MAGIC FROM   table_changes('${catalog}.${schema_gold}.production_source',
# MAGIC                       (SELECT version - 3 FROM (DESCRIBE HISTORY ${catalog}.${schema_gold}.production_source LIMIT 1)),
# MAGIC                       (SELECT version     FROM (DESCRIBE HISTORY ${catalog}.${schema_gold}.production_source LIMIT 1)))
# MAGIC GROUP  BY 1
# MAGIC ORDER  BY 1;
# MAGIC ```
