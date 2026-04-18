# Databricks notebook source

# MAGIC %md
# MAGIC # Gold Strategy 3: CDC MERGE via Delta Change Data Feed
# MAGIC
# MAGIC Reads **CDF change records** from `production_source` starting from the last processed
# MAGIC Delta version (stored in `cdc_state`), then:
# MAGIC 1. Hard-deletes rows that were deleted in the source.
# MAGIC 2. MERGEs inserts and `update_postimage` rows into the gold table.
# MAGIC
# MAGIC State is persisted in `{catalog}.{schema_gold}.cdc_state` keyed by
# MAGIC `table_name = 'production_cdc'`.
# MAGIC
# MAGIC ## When to use
# MAGIC - Source has a mix of inserts, updates, **AND deletes**.
# MAGIC - You need the gold table to be a live, up-to-date **mirror** of the source.
# MAGIC - Source table has CDF enabled (`delta.enableChangeDataFeed = true`).
# MAGIC
# MAGIC ## Trade-offs
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | ✅ | Reflects all change types including hard deletes |
# MAGIC | ✅ | Only reads changed data, not the full source |
# MAGIC | ✅ | Target is a correct mirror — no ghost rows from deleted source data |
# MAGIC | ❌ | Requires CDF on the source and a state table |
# MAGIC | ❌ | MERGE is more expensive per row than a plain append |
# MAGIC
# MAGIC ## CDF Change Types
# MAGIC
# MAGIC | `_change_type` | Meaning | Action taken |
# MAGIC |---|---|---|
# MAGIC | `insert` | New row added to source | MERGE into gold (insert) |
# MAGIC | `update_preimage` | Row value before update | Ignored |
# MAGIC | `update_postimage` | Row value after update | MERGE into gold (update) |
# MAGIC | `delete` | Row hard-deleted from source | DELETE from gold |
# MAGIC
# MAGIC ## Data Flow
# MAGIC ```
# MAGIC cdc_state (last_version = N)
# MAGIC        │
# MAGIC        ▼
# MAGIC table_changes(production_source, N+1, current_version)
# MAGIC        │
# MAGIC        ├── _change_type = 'delete'
# MAGIC        │        └──► DELETE FROM production_cdc WHERE (site_id, production_datetime) IN deletes
# MAGIC        │
# MAGIC        └── _change_type IN ('insert', 'update_postimage')
# MAGIC                  └──► MERGE INTO production_cdc ON (site_id, production_datetime)
# MAGIC                            WHEN MATCHED    → UPDATE ALL
# MAGIC                            WHEN NOT MATCHED → INSERT ALL
# MAGIC        │
# MAGIC        ▼
# MAGIC cdc_state (last_version = current_version)
# MAGIC ```
# MAGIC
# MAGIC ## Output schema
# MAGIC All columns from `production_source`, plus:
# MAGIC - `_cdc_type` STRING — `'insert'` or `'update_postimage'`
# MAGIC - `_cdc_timestamp` TIMESTAMP — `_commit_timestamp` from the CDF record
# MAGIC
# MAGIC ## State table: `cdc_state`
# MAGIC | Column | Type | Description |
# MAGIC |---|---|---|
# MAGIC | `table_name` | STRING (PK) | Identifies the consumer (`'production_cdc'`) |
# MAGIC | `last_version` | LONG | Last Delta version fully processed |
# MAGIC | `updated_at` | TIMESTAMP | When this state was last written |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 0: Imports & Parameters
# MAGIC
# MAGIC | Parameter | Default | Description |
# MAGIC |---|---|---|
# MAGIC | `catalog` | `praju_dev` | Unity Catalog name |
# MAGIC | `schema_gold` | `gold_dev` | Gold schema name |

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import col, current_timestamp, lit

dbutils.widgets.text("catalog",     "praju_dev")
dbutils.widgets.text("schema_gold", "gold_dev")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: State Helpers
# MAGIC
# MAGIC `cdc_state` is a small Delta table that records the last successfully processed
# MAGIC source version.  It is **shared** between `gold_cdc.py` and `gold_no_delete.py`
# MAGIC using different `table_name` keys, so both strategies can coexist.
# MAGIC
# MAGIC `get_last_processed_version` returns `None` on the very first run (state table or
# MAGIC the key row doesn't exist yet), which triggers the full-snapshot bootstrap path.

# COMMAND ----------

STATE_TABLE_KEY = "production_cdc"


def get_last_processed_version(state_table: str) -> int | None:
    """Return the last processed Delta version, or None on first run."""
    if not spark.catalog.tableExists(state_table):
        return None
    rows = spark.sql(
        f"SELECT last_version FROM {state_table} WHERE table_name = '{STATE_TABLE_KEY}'"
    ).collect()
    return rows[0]["last_version"] if rows else None


def save_state(state_table: str, version: int):
    """Upsert the current processed version into cdc_state."""
    df_state = spark.createDataFrame(
        [(STATE_TABLE_KEY, version)],
        schema="table_name STRING, last_version LONG"
    ).withColumn("updated_at", current_timestamp())

    if spark.catalog.tableExists(state_table):
        DeltaTable.forName(spark, state_table).alias("t") \
            .merge(df_state.alias("s"), "t.table_name = s.table_name") \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
    else:
        df_state.write.format("delta").mode("overwrite").saveAsTable(state_table)

    print(f"  ✓ State saved: {state_table}  last_version={version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Parameters

# COMMAND ----------

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Get CDF Range
# MAGIC
# MAGIC Determines the Delta version range to read:
# MAGIC - `start_version = last_version + 1` (exclusive of already-processed versions)
# MAGIC - `end_version = current_version` (latest committed version)
# MAGIC
# MAGIC If `start_version > current_version`, there are no new changes and the script exits early.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: First-Run Bootstrap
# MAGIC
# MAGIC CDF records only exist **after** `delta.enableChangeDataFeed` was set.
# MAGIC Reading from version 0 would raise an error.
# MAGIC
# MAGIC On the first run (no state row), we take a **full snapshot** of the source and
# MAGIC treat every row as an insert.  The current version is saved as the baseline so
# MAGIC subsequent runs can read CDF from `last_version + 1` onward.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Delete Hard Deletes
# MAGIC
# MAGIC Rows with `_change_type = 'delete'` are hard-deleted from the gold table using
# MAGIC a correlated subquery on `(site_id, production_datetime)`.
# MAGIC
# MAGIC This is what distinguishes `gold_cdc.py` from `gold_no_delete.py` — deleted
# MAGIC source rows are removed from this gold table.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: MERGE Upserts
# MAGIC
# MAGIC Rows with `_change_type IN ('insert', 'update_postimage')` are merged into the
# MAGIC gold table using `WHEN MATCHED UPDATE` / `WHEN NOT MATCHED INSERT`.
# MAGIC
# MAGIC `update_preimage` rows are excluded (they represent the old value before an update
# MAGIC and are not needed in the gold table).

# COMMAND ----------

def run_cdc(catalog: str, schema_gold: str):
    source_table = f"{catalog}.{schema_gold}.production_source"
    target_table = f"{catalog}.{schema_gold}.production_cdc"
    state_table  = f"{catalog}.{schema_gold}.cdc_state"

    current_version = spark.sql(
        f"DESCRIBE HISTORY {source_table} LIMIT 1"
    ).collect()[0]["version"]

    last_version = get_last_processed_version(state_table)

    # ---- First run: bootstrap from full table snapshot, no CDF needed ----
    # CDF is only recorded after the TBLPROPERTIES enable, so reading from
    # version 0 would fail.  Instead, take a full snapshot and record the
    # current version as our baseline.
    if last_version is None or not spark.catalog.tableExists(target_table):
        print(f"First run — bootstrapping full snapshot at version {current_version}")
        source_cols = [c for c in spark.table(source_table).columns]
        df_snapshot = spark.table(source_table) \
            .select(*source_cols,
                    lit("insert").alias("_cdc_type"),
                    current_timestamp().alias("_cdc_timestamp"))
        df_snapshot.write.format("delta").mode("overwrite").saveAsTable(target_table)
        save_state(state_table, current_version)
        print(f"✓ {target_table} — {spark.table(target_table).count():,} rows (bootstrap).")
        return

    # ---- Subsequent runs: use CDF ----
    start_version = last_version + 1
    print(f"CDF range: version {start_version} → {current_version}")

    if start_version > current_version:
        print("No new Delta versions to process. Exiting.")
        return

    df_changes = spark.read.format("delta") \
        .option("readChangeFeed",  "true") \
        .option("startingVersion", start_version) \
        .option("endingVersion",   current_version) \
        .table(source_table)

    # Source columns (strip CDF metadata columns before writing)
    source_cols = [
        c for c in df_changes.columns
        if not c.startswith("_change") and not c.startswith("_commit")
    ]

    df_deletes  = df_changes.filter(col("_change_type") == "delete")
    df_upserts  = df_changes.filter(col("_change_type").isin("insert", "update_postimage")) \
        .select(*source_cols,
                col("_change_type").alias("_cdc_type"),
                col("_commit_timestamp").alias("_cdc_timestamp"))

    delete_count = df_deletes.count()
    upsert_count = df_upserts.count()
    print(f"Changes found — deletes: {delete_count:,}  upserts: {upsert_count:,}")

    # 1. Hard-delete rows removed from source
    if delete_count > 0:
        df_deletes.select("site_id", "production_datetime") \
            .createOrReplaceTempView("_cdc_deletes_vw")
        spark.sql(f"""
            DELETE FROM {target_table} t
            WHERE EXISTS (
                SELECT 1 FROM _cdc_deletes_vw s
                WHERE s.site_id = t.site_id AND s.production_datetime = t.production_datetime
            )
        """)
        print(f"  Deleted {delete_count:,} rows from {target_table}.")

    # 2. MERGE inserts + updates
    if upsert_count > 0:
        DeltaTable.forName(spark, target_table).alias("t") \
            .merge(
                df_upserts.alias("s"),
                "t.site_id = s.site_id AND t.production_datetime = s.production_datetime"
            ) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
        print(f"  Merged {upsert_count:,} upsert rows into {target_table}.")

    save_state(state_table, current_version)
    print(f"✓ {target_table} — {spark.table(target_table).count():,} rows.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Run

# COMMAND ----------

catalog     = dbutils.widgets.get("catalog")
schema_gold = dbutils.widgets.get("schema_gold")

print(f"catalog={catalog}  schema_gold={schema_gold}")
run_cdc(catalog, schema_gold)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation
# MAGIC
# MAGIC ```sql
# MAGIC -- 1. Gold row count vs source (gold should be fewer if deletes were applied)
# MAGIC SELECT
# MAGIC     (SELECT COUNT(*) FROM ${catalog}.${schema_gold}.production_cdc)    AS gold_rows,
# MAGIC     (SELECT COUNT(*) FROM ${catalog}.${schema_gold}.production_source) AS source_rows;
# MAGIC
# MAGIC -- 2. Check state table
# MAGIC SELECT * FROM ${catalog}.${schema_gold}.cdc_state
# MAGIC WHERE table_name = 'production_cdc';
# MAGIC
# MAGIC -- 3. CDC type breakdown in gold
# MAGIC SELECT _cdc_type, COUNT(*) AS cnt
# MAGIC FROM   ${catalog}.${schema_gold}.production_cdc
# MAGIC GROUP  BY 1;
# MAGIC
# MAGIC -- 4. Spot-check: a deleted source row should NOT exist in gold
# MAGIC -- (Run after simulate_changes.py has applied deletes)
# MAGIC SELECT COUNT(*) AS should_be_zero
# MAGIC FROM   ${catalog}.${schema_gold}.production_cdc g
# MAGIC WHERE  NOT EXISTS (
# MAGIC     SELECT 1 FROM ${catalog}.${schema_gold}.production_source s
# MAGIC     WHERE s.site_id = g.site_id AND s.production_datetime = g.production_datetime
# MAGIC );
# MAGIC ```
