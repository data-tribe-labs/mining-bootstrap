# Databricks notebook source
# MAGIC %md
# MAGIC # Module 2 — Bronze Ingest with Auto Loader
# MAGIC
# MAGIC We ingest three datasets from the **shared landing Volume** (the "hub") into your
# MAGIC own `bronze` schema (your "spoke"):
# MAGIC
# MAGIC | Source Parquet | → Bronze table |
# MAGIC |---|---|
# MAGIC | `landing/mining_sample/mine_sites/` | `bronze.mine_sites` |
# MAGIC | `landing/mining_sample/production/` | `bronze.daily_production` |
# MAGIC | `landing/mining_sample/equipment_maintenance/` | `bronze.equipment_maintenance` |
# MAGIC
# MAGIC ## Ingestion patterns at a glance
# MAGIC
# MAGIC Auto Loader is one of many ingestion patterns on Databricks. Picking the right tool
# MAGIC for the source matters more than picking the most advanced tool. A quick menu:
# MAGIC
# MAGIC | Pattern | Best for | Example use case |
# MAGIC |---|---|---|
# MAGIC | **Auto Loader** (today) | Continuous or large-scale file ingest from cloud storage | Drone survey outputs landing in S3 hourly |
# MAGIC | **`COPY INTO`** | Simple idempotent batch file loads | Daily vendor file drop into a Volume |
# MAGIC | **Lakeflow Connect** | Managed SaaS + CDC from operational databases | SAP, Oracle, Maximo, ServiceNow, SharePoint |
# MAGIC | **REST API via a Python Job task** | Custom APIs with no managed connector | Weather / geology / equipment-vendor APIs |
# MAGIC | **JDBC read** (`spark.read.format("jdbc")`) | One-off or periodic pulls from an OLTP DB | Nightly snapshot of an internal SQL Server table |
# MAGIC | **Kafka / Event Hubs / Kinesis / Pub/Sub** | Real-time event streams | SCADA telemetry, IoT from mine sites |
# MAGIC | **Lakehouse Federation** | Zero-copy query of another system | Query a Snowflake catalog directly from UC |
# MAGIC | **Delta Sharing** | Consume a provider's dataset, no copy | Receive data shared by a partner org |
# MAGIC | **File-arrival trigger on Workflows** | Event-driven batch | Fire a job the moment a file lands in a Volume |
# MAGIC | **Third-party (Fivetran, Airbyte, Rivery, …)** | Breadth of SaaS sources Lakeflow Connect doesn't yet cover | Fall-back for long-tail SaaS apps |
# MAGIC
# MAGIC Today we're using Auto Loader because our source is a Volume full of Parquet files —
# MAGIC here's why it's the right fit.
# MAGIC
# MAGIC ## Why Auto Loader?
# MAGIC
# MAGIC Picture the naive way to ingest from cloud storage: `LIST` the folder, diff against
# MAGIC what you've already loaded, read the rest. That works for ten files. At millions
# MAGIC of files — and especially on S3, where `LIST` is slow and rate-limited — it collapses.
# MAGIC Auto Loader is Databricks' managed answer.
# MAGIC
# MAGIC ### Benefits
# MAGIC
# MAGIC **1. Scalable file discovery**
# MAGIC   - Default: *directory listing* — cheap, no setup, fine up to ~millions of files
# MAGIC   - Optional: *file-notification* mode — Databricks provisions an SNS/SQS (AWS) or
# MAGIC     EventGrid (Azure) queue. Put-object events flow into the queue; the stream
# MAGIC     reads from the queue instead of `LIST`ing. Scales to **billions** of files.
# MAGIC
# MAGIC **2. Exactly-once ingestion**
# MAGIC   - A RocksDB-backed checkpoint records every file path that's been processed
# MAGIC   - Re-running the stream is a no-op on files it's already seen
# MAGIC
# MAGIC **3. Schema inference**
# MAGIC   - First micro-batch samples files to infer column types
# MAGIC   - Supports JSON, CSV, Parquet, Avro, ORC, binary files
# MAGIC   - No `StructType(...)` boilerplate like you'd write for plain `spark.read`
# MAGIC
# MAGIC **4. Schema evolution**
# MAGIC   - Default mode `addNewColumns`: if a new column appears in source, the stream
# MAGIC     fails **once**, records the new schema, and restarts with it
# MAGIC   - Other modes: `rescue` (new columns go into `_rescued_data`), `failOnNewColumns`,
# MAGIC     `none`
# MAGIC
# MAGIC **5. Rescued-data column**
# MAGIC   - Values that don't match the inferred schema (e.g. string in an int column)
# MAGIC     aren't dropped — they're preserved in `_rescued_data` for you to fix later
# MAGIC
# MAGIC **6. Backfill support**
# MAGIC   - `includeExistingFiles=true` processes the full history on first run
# MAGIC   - Subsequent runs only pick up new files (checkpoint remembers)
# MAGIC   - Same pipeline serves historical backfill AND ongoing ingest
# MAGIC
# MAGIC **7. Resilient to source reshaping**
# MAGIC   - Files moved between subfolders, renamed, etc. — paths are tracked, not inode-level state
# MAGIC
# MAGIC **8. Streaming or batch — same code**
# MAGIC
# MAGIC   - trigger(availableNow=True) — "catch up and stop". When the cell runs, Auto Loader picks up every file the checkpoint hasn't seen yet, processes them, then exits. The cluster can be torn down. Feels identical to a nightly batch job. This is what Module 2 uses, because a workshop attendee running a cell expects "run → finish". You then schedule this in a Workflow to run hourly/daily and it behaves like a classic batch pipeline.
# MAGIC   - trigger(processingTime='1 minute') — "stay running forever". The stream loops every minute: check for new files, process them, sleep, repeat. The cluster stays up. Use this when downstream consumers need fresh data within a minute or two.
# MAGIC   - trigger(continuous='1 second') — sub-second latency. Experimental, rarely what you want.
# MAGIC
# MAGIC ### Under the hood (for the curious)
# MAGIC
# MAGIC - It's a **Structured Streaming source** called `cloudFiles` — plugs into any
# MAGIC   Spark streaming pipeline
# MAGIC - The **checkpoint** at `checkpointLocation` is a RocksDB state store holding the
# MAGIC   set of ingested file paths + their offsets. It's portable — copy the checkpoint
# MAGIC   folder to another cluster and the pipeline resumes exactly where it left off.
# MAGIC - The **schema location** holds a JSON file of the inferred schema, versioned each
# MAGIC   time it evolves — you can see the history of schema changes on disk
# MAGIC - In **file-notification mode**, an SNS topic fans out to an SQS queue that the
# MAGIC   stream long-polls; no coordinator, no bottleneck
# MAGIC - **Parallelism** — files are assigned to micro-batches and distributed across all
# MAGIC   executors; no single-leader bottleneck
# MAGIC
# MAGIC ### Contrast — what you'd build yourself without it
# MAGIC
# MAGIC - Maintain your own "files processed" tracking table
# MAGIC - Pay `LIST` costs on every run
# MAGIC - Handle partial-batch failures
# MAGIC - Detect schema drift manually
# MAGIC - Race conditions when files land mid-read
# MAGIC - Build the backfill path separately from the incremental path
# MAGIC
# MAGIC Each stream below uses `trigger(availableNow=True)` so it runs once, catches up all
# MAGIC available files, and stops — perfect for a scheduled job.
# MAGIC
# MAGIC **More:** https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/

# COMMAND ----------

# MAGIC %md
# MAGIC ## Config — edit this cell

# COMMAND ----------

# EDIT THIS — point to your own catalog
CATALOG = "worley_firstname_lastname"

SCHEMA_BRONZE = "bronze"
LANDING_VOLUME = "/Volumes/workshop_shared/landing/mining_sample"

# Metadata Volume (created in Module 1) — Auto Loader schema + checkpoint live here
METADATA_BASE = f"/Volumes/{CATALOG}/{SCHEMA_BRONZE}/metadata"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: The Auto Loader helper
# MAGIC
# MAGIC One function wraps the `cloudFiles` format. We'll call it three times — once per table.

# COMMAND ----------

def ingest_to_bronze(
    source_path: str,
    target_table: str,
    source_format: str = "parquet",
) -> None:
    """Stream a directory of files into a bronze Delta table using Auto Loader."""

    schema_location = f"{METADATA_BASE}/{target_table}/_schema"
    checkpoint_location = f"{METADATA_BASE}/{target_table}/_checkpoint"

    print(f"Ingesting  {source_path}")
    print(f"        →  {target_table}")

    (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", source_format)
        .option("cloudFiles.schemaLocation", schema_location)
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.rescuedDataColumn", "_rescued_data")
        .option("cloudFiles.includeExistingFiles", "true")
        .load(source_path)
        .writeStream
        .format("delta")
        .option("checkpointLocation", checkpoint_location)
        .outputMode("append")
        .trigger(availableNow=True)
        .toTable(target_table)
        .awaitTermination()
    )

    rows = spark.table(target_table).count()
    print(f"        ✓  {rows:,} rows in {target_table}\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Ingest `mine_sites` (50 rows — warm-up)

# COMMAND ----------

ingest_to_bronze(
    source_path=f"{LANDING_VOLUME}/mine_sites",
    target_table=f"{CATALOG}.{SCHEMA_BRONZE}.mine_sites",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Ingest `equipment_maintenance` (~500 rows)

# COMMAND ----------

ingest_to_bronze(
    source_path=f"{LANDING_VOLUME}/equipment_maintenance",
    target_table=f"{CATALOG}.{SCHEMA_BRONZE}.equipment_maintenance",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Ingest `daily_production` (~175M rows — the big one)
# MAGIC
# MAGIC On serverless this takes 3-5 minutes. Auto Loader discovers all the date-partitioned
# MAGIC Parquet files and streams them in. Re-running the cell is a no-op because the
# MAGIC checkpoint already knows what's been processed.

# COMMAND ----------

ingest_to_bronze(
    source_path=f"{LANDING_VOLUME}/production",
    target_table=f"{CATALOG}.{SCHEMA_BRONZE}.daily_production",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Peek at the data

# COMMAND ----------

display(spark.table(f"{CATALOG}.{SCHEMA_BRONZE}.mine_sites").limit(10))

# COMMAND ----------

display(spark.table(f"{CATALOG}.{SCHEMA_BRONZE}.equipment_maintenance").limit(10))

# COMMAND ----------

display(spark.table(f"{CATALOG}.{SCHEMA_BRONZE}.daily_production").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Recap
# MAGIC
# MAGIC Three bronze Delta tables in your catalog, each backed by an Auto Loader stream
# MAGIC with checkpoint + schema tracking. If new files land in the shared Volume later,
# MAGIC re-running this notebook will pick them up — that's the streaming-on-demand pattern.
# MAGIC
# MAGIC Next: Module 3 — cleanse these into silver.
