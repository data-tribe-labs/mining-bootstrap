# Databricks notebook source

# MAGIC %md
# MAGIC # Gold Strategy 4: Incremental Append (No Deletes — Audit Log)
# MAGIC
# MAGIC Reads CDF change records from `production_source` and appends **only inserts and
# MAGIC `update_postimage` rows** to the gold table.  Delete and `update_preimage` records
# MAGIC are intentionally discarded.
# MAGIC
# MAGIC The target table grows monotonically and preserves the **full audit trail** of every
# MAGIC insert and update — rows are never removed, even if they were deleted from the source.
# MAGIC
# MAGIC ## When to use
# MAGIC - You need an immutable audit log of every insert and update.
# MAGIC - Downstream consumers need "what was the value at time T?" queries.
# MAGIC - Data-retention or compliance rules **prohibit deleting records** from gold.
# MAGIC
# MAGIC ## Trade-offs
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | ✅ | Append-only — extremely fast writes, no MERGE overhead |
# MAGIC | ✅ | Preserves complete INSERT and UPDATE history |
# MAGIC | ❌ | Target row count only ever grows; source deletes are invisible here |
# MAGIC | ❌ | Requires deduplication in downstream consumers for latest-value queries |
# MAGIC
# MAGIC ## Key difference vs `gold_cdc.py`
# MAGIC
# MAGIC > **Deletes in the source are intentionally ignored.**
# MAGIC > A row inserted into `production_source` will always appear in this gold table,
# MAGIC > even after it has been deleted from the source.
# MAGIC
# MAGIC ## Data Flow
# MAGIC ```
# MAGIC table_changes(production_source, N+1, current_version)
# MAGIC        │
# MAGIC        ├── _change_type = 'insert'           → APPEND to production_no_delete
# MAGIC        ├── _change_type = 'update_postimage' → APPEND to production_no_delete
# MAGIC        ├── _change_type = 'update_preimage'  → DISCARD (old value, not needed)
# MAGIC        └── _change_type = 'delete'           → DISCARD (audit log never shrinks)
# MAGIC ```
# MAGIC
# MAGIC ## Output schema
# MAGIC All columns from `production_source`, plus:
# MAGIC - `_operation` STRING — `'INSERT'` or `'UPDATE_POSTIMAGE'`
# MAGIC - `_commit_version` LONG — Delta version of the change
# MAGIC - `_commit_timestamp` TIMESTAMP — when the change was committed
# MAGIC
# MAGIC ## State table: `cdc_state` (shared with `gold_cdc.py`)
# MAGIC | Column | Type | Description |
# MAGIC |---|---|---|
# MAGIC | `table_name` | STRING (PK) | `'production_no_delete'` for this strategy |
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
from pyspark.sql.functions import col, current_timestamp, lit, upper

dbutils.widgets.text("catalog",     "praju_dev")
dbutils.widgets.text("schema_gold", "gold_dev")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: State Helpers
# MAGIC
# MAGIC Same pattern as `gold_cdc.py` — each script is self-contained and shares the
# MAGIC `cdc_state` table using a different `table_name` key (`'production_no_delete'`).

# COMMAND ----------

STATE_TABLE_KEY = "production_no_delete"


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
# MAGIC Same version-range logic as `gold_cdc.py`:
# MAGIC - `start_version = last_version + 1`
# MAGIC - `end_version = current_version`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: First-Run Bootstrap
# MAGIC
# MAGIC On the first run, CDF history may not be available from version 0.
# MAGIC Bootstrap by reading the full source snapshot and labeling every row as `INSERT`.
# MAGIC Save `current_version` as the baseline for the next CDF read.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Append Inserts + Updates Only
# MAGIC
# MAGIC Filter the CDF stream to `_change_type IN ('insert', 'update_postimage')` and
# MAGIC append directly to the target — no MERGE, no DELETE.
# MAGIC
# MAGIC `_change_type` is uppercased to `_operation` for readability (`'INSERT'`, `'UPDATE_POSTIMAGE'`).
# MAGIC
# MAGIC If there are no rows to append in this CDF range, state is still saved so the
# MAGIC next run starts from the correct version.

# COMMAND ----------

def run_no_delete(catalog: str, schema_gold: str):
    source_table = f"{catalog}.{schema_gold}.production_source"
    target_table = f"{catalog}.{schema_gold}.production_no_delete"
    state_table  = f"{catalog}.{schema_gold}.cdc_state"

    current_version = spark.sql(
        f"DESCRIBE HISTORY {source_table} LIMIT 1"
    ).collect()[0]["version"]

    last_version = get_last_processed_version(state_table)

    # ---- First run: bootstrap from full table snapshot, no CDF needed ----
    # CDF is only recorded after the TBLPROPERTIES enable, so reading from
    # version 0 would fail.  Bootstrap by treating all current rows as INSERTs.
    if last_version is None or not spark.catalog.tableExists(target_table):
        print(f"First run — bootstrapping full snapshot at version {current_version}")
        source_cols = spark.table(source_table).columns
        df_snapshot = spark.table(source_table) \
            .select(*source_cols,
                    lit("INSERT").alias("_operation"))
        df_snapshot.write.format("delta").mode("overwrite").saveAsTable(target_table)
        save_state(state_table, current_version)
        total_rows = spark.table(target_table).count()
        print(f"✓ {target_table} — {total_rows:,} rows (bootstrap).")
        return

    # ---- Subsequent runs: use CDF, filter to inserts + updates only ----
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

    # Source columns (strip CDF metadata columns)
    source_cols = [
        c for c in df_changes.columns
        if not c.startswith("_change") and not c.startswith("_commit")
    ]

    df_to_append = df_changes \
        .filter(col("_change_type").isin("insert", "update_postimage")) \
        .select(
            *source_cols,
            upper(col("_change_type")).alias("_operation"),
        )

    append_count = df_to_append.count()
    print(f"Rows to append (inserts + update_postimage): {append_count:,}")

    if append_count == 0:
        print("No insert/update rows in this CDF range. Saving state and exiting.")
        save_state(state_table, current_version)
        return

    df_to_append.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(target_table)

    save_state(state_table, current_version)
    total_rows = spark.table(target_table).count()
    print(f"✓ {target_table} — {total_rows:,} total rows (audit log, ever-growing).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Run

# COMMAND ----------

catalog     = dbutils.widgets.get("catalog")
schema_gold = dbutils.widgets.get("schema_gold")

print(f"catalog={catalog}  schema_gold={schema_gold}")
run_no_delete(catalog, schema_gold)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation
# MAGIC
# MAGIC ```sql
# MAGIC -- 1. Operation breakdown — should show INSERT and UPDATE_POSTIMAGE, never DELETE
# MAGIC SELECT _operation, COUNT(*) AS cnt
# MAGIC FROM   ${catalog}.${schema_gold}.production_no_delete
# MAGIC GROUP  BY 1
# MAGIC ORDER  BY 1;
# MAGIC
# MAGIC -- 2. Rows from deleted source records still exist here (audit log is immutable)
# MAGIC SELECT COUNT(*) AS audit_rows_for_deleted_sources
# MAGIC FROM   ${catalog}.${schema_gold}.production_no_delete g
# MAGIC WHERE  NOT EXISTS (
# MAGIC     SELECT 1 FROM ${catalog}.${schema_gold}.production_source s
# MAGIC     WHERE s.site_id = g.site_id AND s.production_datetime = g.production_datetime
# MAGIC );
# MAGIC
# MAGIC -- 3. Check state table
# MAGIC SELECT * FROM ${catalog}.${schema_gold}.cdc_state
# MAGIC WHERE table_name = 'production_no_delete';
# MAGIC
# MAGIC -- 4. Latest value per (site_id, production_datetime) — dedup for point-in-time queries
# MAGIC SELECT *
# MAGIC FROM (
# MAGIC     SELECT *, ROW_NUMBER() OVER (
# MAGIC         PARTITION BY site_id, production_datetime
# MAGIC         ORDER BY _commit_version DESC
# MAGIC     ) AS rn
# MAGIC     FROM ${catalog}.${schema_gold}.production_no_delete
# MAGIC )
# MAGIC WHERE rn = 1
# MAGIC LIMIT 10;
# MAGIC ```
