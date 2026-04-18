# Databricks notebook source
# MAGIC %md
# MAGIC # Module 3 — Silver Transforms (PySpark streaming)
# MAGIC
# MAGIC Silver is where bronze becomes useable. Two streams, both PySpark:
# MAGIC
# MAGIC | Bronze source | → Silver target | Pattern |
# MAGIC |---|---|---|
# MAGIC | `bronze.daily_production` | `silver.daily_production` | Cleanse + type cast |
# MAGIC | `bronze.equipment_maintenance` | `silver.equipment_maintenance_snapshot` | **Upsert** (SCD1) via `MERGE` in `foreachBatch` |
# MAGIC
# MAGIC We'll skip the `_rescued_data` column (bronze safety-net) and make sure silver rows
# MAGIC are typed and unique on their business key.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Config — edit this cell

# COMMAND ----------

# EDIT THIS — point to your own catalog
CATALOG = "workshop_firstname_lastname"

SCHEMA_BRONZE = "bronze"
SCHEMA_SILVER = "silver"

# Checkpoints for silver streams (separate from bronze)
CHECKPOINT_BASE = f"/Volumes/{CATALOG}/{SCHEMA_BRONZE}/metadata/silver"

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import col

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: `silver.daily_production` — cleanse + cast
# MAGIC
# MAGIC Streaming read from bronze, drop the rescued-data column, keep everything else.
# MAGIC A simple pass-through is enough here — the bronze schema is already correctly typed
# MAGIC from the Parquet source. In a real project silver is also where you'd add data
# MAGIC quality filters (nulls, out-of-range values, etc.).

# COMMAND ----------

def load_silver_daily_production() -> None:
    bronze = f"{CATALOG}.{SCHEMA_BRONZE}.daily_production"
    silver = f"{CATALOG}.{SCHEMA_SILVER}.daily_production"

    (
        spark.readStream.table(bronze)
        .drop("_rescued_data")
        .filter(col("tons_extracted") >= 0)  # DQ: drop negative tons
        .filter(col("mineral_grade_percent").between(0, 100))
        .writeStream
        .format("delta")
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/daily_production/_checkpoint")
        .outputMode("append")
        .trigger(availableNow=True)
        .toTable(silver)
        .awaitTermination()
    )

    print(f"✓ {silver}  —  {spark.table(silver).count():,} rows")

load_silver_daily_production()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: `silver.equipment_maintenance_snapshot` — upsert via MERGE
# MAGIC
# MAGIC Equipment records change over time (health score, maintenance dates, status). We want
# MAGIC silver to always hold **one row per `equipment_id`** — the latest.
# MAGIC
# MAGIC Streaming `append` mode can't do updates on its own, so we use `foreachBatch` to run
# MAGIC a Delta `MERGE` inside each micro-batch:
# MAGIC
# MAGIC - **Matched** → update the existing row with the latest attributes
# MAGIC - **Not matched** → insert
# MAGIC
# MAGIC This is the "upsert" / SCD Type 1 pattern. (Full SCD Type 2 — keeping a history of
# MAGIC past versions — adds `__START_AT`/`__END_AT` columns; we're skipping that here to
# MAGIC keep the module tight.)

# COMMAND ----------

def ensure_snapshot_target() -> None:
    """Create the target table once, with the schema we want. Idempotent."""
    target = f"{CATALOG}.{SCHEMA_SILVER}.equipment_maintenance_snapshot"
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {target} (
            equipment_id INT,
            site_id INT,
            equipment_type STRING,
            category STRING,
            equipment_name STRING,
            install_date DATE,
            operating_hours INT,
            last_maintenance_date DATE,
            next_maintenance_date DATE,
            health_score INT,
            status STRING,
            annual_maintenance_cost_usd DOUBLE
        ) USING DELTA
    """)

ensure_snapshot_target()

# COMMAND ----------

def upsert_batch(batch_df, batch_id: int) -> None:
    """foreachBatch handler — merge this micro-batch into the silver snapshot table."""
    target = f"{CATALOG}.{SCHEMA_SILVER}.equipment_maintenance_snapshot"
    target_dt = DeltaTable.forName(spark, target)

    # Dedup within the batch itself: keep the most recent row per equipment_id
    from pyspark.sql import Window
    from pyspark.sql.functions import row_number, desc

    latest_in_batch = (
        batch_df.drop("_rescued_data")
        .withColumn(
            "_rn",
            row_number().over(
                Window.partitionBy("equipment_id")
                .orderBy(desc("last_maintenance_date"))
            ),
        )
        .filter("_rn = 1")
        .drop("_rn")
    )

    (
        target_dt.alias("t")
        .merge(latest_in_batch.alias("s"), "t.equipment_id = s.equipment_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Alternative — the same MERGE in Spark SQL
# MAGIC
# MAGIC Some teams prefer SQL for readability. The handler below is functionally identical
# MAGIC to `upsert_batch` above — it just expresses the dedup-and-merge in SQL rather than
# MAGIC the DataFrame / DeltaTable API.
# MAGIC
# MAGIC The trick is registering the incoming micro-batch as a temp view so SQL can see it.

# COMMAND ----------

def upsert_batch_sql(batch_df, batch_id: int) -> None:
    """Spark SQL variant of upsert_batch — same semantics, different syntax."""
    target = f"{CATALOG}.{SCHEMA_SILVER}.equipment_maintenance_snapshot"

    # Make the micro-batch available to SQL as a temp view
    batch_df.drop("_rescued_data").createOrReplaceTempView("incoming_equipment")

    spark.sql(f"""
        MERGE INTO {target} AS t
        USING (
            SELECT *
            FROM (
                SELECT
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY equipment_id
                        ORDER BY last_maintenance_date DESC
                    ) AS _rn
                FROM incoming_equipment
            )
            WHERE _rn = 1
        ) AS s
        ON t.equipment_id = s.equipment_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

# To use this variant instead, change `.foreachBatch(upsert_batch)` to
# `.foreachBatch(upsert_batch_sql)` in the next cell.

# COMMAND ----------

def load_silver_equipment_snapshot() -> None:
    bronze = f"{CATALOG}.{SCHEMA_BRONZE}.equipment_maintenance"
    silver = f"{CATALOG}.{SCHEMA_SILVER}.equipment_maintenance_snapshot"

    (
        spark.readStream.table(bronze)
        .writeStream
        .foreachBatch(upsert_batch)
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/equipment_snapshot/_checkpoint")
        .trigger(availableNow=True)
        .start()
        .awaitTermination()
    )

    print(f"✓ {silver}  —  {spark.table(silver).count():,} rows")

load_silver_equipment_snapshot()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Peek at silver

# COMMAND ----------

display(spark.sql(f"""
    SELECT site_id, COUNT(*) AS daily_rows, MIN(production_date) AS earliest, MAX(production_date) AS latest
    FROM {CATALOG}.{SCHEMA_SILVER}.daily_production
    GROUP BY site_id
    ORDER BY site_id
    LIMIT 10
"""))

# COMMAND ----------

display(spark.table(f"{CATALOG}.{SCHEMA_SILVER}.equipment_maintenance_snapshot").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Recap
# MAGIC
# MAGIC - `silver.daily_production` — streaming cleanse + DQ filters, append-only
# MAGIC - `silver.equipment_maintenance_snapshot` — streaming upsert via `MERGE`, one row per equipment
# MAGIC
# MAGIC Next: Module 4 — build gold aggregates and protect them with column + row masks.
