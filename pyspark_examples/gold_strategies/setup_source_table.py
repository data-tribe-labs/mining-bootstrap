# Databricks notebook source

# MAGIC %md
# MAGIC # Gold Strategies — Step 1: Create Source Table
# MAGIC
# MAGIC This script creates the Delta table `{catalog}.{schema_gold}.production_source` by reading
# MAGIC from `{catalog}.{schema_silver}.daily_production`.
# MAGIC
# MAGIC **When to run:** Once, before any of the four gold load strategy scripts.
# MAGIC It is fully **idempotent** — if the source table already exists it prints the row count and exits.
# MAGIC
# MAGIC **Why a separate source table?**
# MAGIC The silver table is the canonical record of production data. By copying it into
# MAGIC `production_source` (with CDF enabled) we can apply synthetic changes via
# MAGIC `simulate_changes.py` without touching silver, and the four gold strategies can
# MAGIC each read incremental CDF records independently.
# MAGIC
# MAGIC ## Data Flow
# MAGIC ```
# MAGIC silver.daily_production
# MAGIC        │
# MAGIC        │  overwrite + partitionBy(production_date)
# MAGIC        ▼
# MAGIC gold.production_source   ← CDF enabled (delta.enableChangeDataFeed = true)
# MAGIC        │
# MAGIC        ├──► gold_full_refresh.py
# MAGIC        ├──► gold_incremental.py
# MAGIC        ├──► gold_cdc.py
# MAGIC        └──► gold_no_delete.py
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 0: Imports & Parameters
# MAGIC
# MAGIC `spark` is provided by the Databricks runtime (no explicit `SparkSession` needed).
# MAGIC
# MAGIC Parameters are read from job `base_parameters` via `dbutils.widgets`.
# MAGIC Defaults allow the notebook to be run interactively without a job context.
# MAGIC
# MAGIC | Parameter | Default | Description |
# MAGIC |---|---|---|
# MAGIC | `catalog` | `praju_dev` | Unity Catalog name |
# MAGIC | `schema_silver` | `silver` | Silver schema name |
# MAGIC | `schema_gold` | `gold_dev` | Gold schema name |

# COMMAND ----------

dbutils.widgets.text("catalog",       "praju_dev")
dbutils.widgets.text("schema_silver", "silver")
dbutils.widgets.text("schema_gold",   "gold_dev")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create Catalog and Schemas
# MAGIC
# MAGIC Ensures the Unity Catalog and both schemas exist before any table operations.
# MAGIC All three `CREATE ... IF NOT EXISTS` statements are safe to run repeatedly.

# COMMAND ----------

def create_catalog_and_schemas(catalog: str, schema_silver: str, schema_gold: str):
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema_silver}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema_gold}")
    print(f"✓ Catalog/schemas ready: {catalog}.{schema_silver}, {catalog}.{schema_gold}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create the Source Table
# MAGIC
# MAGIC Reads `silver.daily_production`, writes it to `gold.production_source` as a
# MAGIC **partitioned Delta table**, then explicitly enables **Change Data Feed (CDF)**.
# MAGIC
# MAGIC CDF records every row-level change (insert / update / delete) as metadata that
# MAGIC downstream scripts can read via `spark.read.format("delta").option("readChangeFeed", "true")`.
# MAGIC
# MAGIC If the table already exists the function short-circuits to keep the script idempotent.

# COMMAND ----------

def setup_source_table(catalog: str, schema_silver: str, schema_gold: str):
    """
    Reads silver.daily_production and writes it as a partitioned Delta table
    with CDF enabled.  Skips creation if the table already exists.
    """
    source_table = f"{catalog}.{schema_silver}.daily_production"
    target_table = f"{catalog}.{schema_gold}.production_source"

    if spark.catalog.tableExists(target_table):
        row_count = spark.table(target_table).count()
        print(f"✓ {target_table} already exists — {row_count:,} rows. Skipping creation.")
        return

    print(f"Reading source: {source_table}")
    df = spark.table(source_table)

    print(f"Writing target: {target_table}  (partitioned by production_date)")
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("production_date") \
        .saveAsTable(target_table)

    # Belt-and-suspenders: enable CDF in case the table property wasn't
    # inherited from the source.
    spark.sql(
        f"ALTER TABLE {target_table} "
        f"SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')"
    )

    row_count = spark.table(target_table).count()
    print(f"✓ Created {target_table} — {row_count:,} rows, CDF enabled.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Run

# COMMAND ----------

catalog       = dbutils.widgets.get("catalog")
schema_silver = dbutils.widgets.get("schema_silver")
schema_gold   = dbutils.widgets.get("schema_gold")

print(f"catalog={catalog}  schema_silver={schema_silver}  schema_gold={schema_gold}")
create_catalog_and_schemas(catalog, schema_silver, schema_gold)
setup_source_table(catalog, schema_silver, schema_gold)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation
# MAGIC
# MAGIC Run the queries below to confirm the table was created correctly.
# MAGIC
# MAGIC ```sql
# MAGIC -- 1. Row count
# MAGIC SELECT COUNT(*) AS row_count FROM ${catalog}.${schema_gold}.production_source;
# MAGIC
# MAGIC -- 2. Verify CDF is enabled (look for delta.enableChangeDataFeed = true)
# MAGIC DESCRIBE TABLE EXTENDED ${catalog}.${schema_gold}.production_source;
# MAGIC
# MAGIC -- 3. Inspect partition distribution
# MAGIC SELECT production_date, COUNT(*) AS rows
# MAGIC FROM   ${catalog}.${schema_gold}.production_source
# MAGIC GROUP  BY 1
# MAGIC ORDER  BY 1 DESC
# MAGIC LIMIT  10;
# MAGIC ```
