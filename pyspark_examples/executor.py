# Databricks notebook source

# MAGIC %md
# MAGIC # Dispatch Pipeline — Core-level Executor
# MAGIC
# MAGIC Dispatches one unit of work to each core using `mapInPandas`.
# MAGIC Spark schedules one task per partition, and each task runs on one core.
# MAGIC
# MAGIC ## How it works
# MAGIC
# MAGIC ```
# MAGIC spark.range(0, N, 1, numPartitions=N)   ← DataFrame with N partitions, one row each
# MAGIC        │
# MAGIC        │  mapInPandas(work_on_core)
# MAGIC        │        called once per partition → once per core
# MAGIC        ▼
# MAGIC [ core_0, core_1, ..., core_N-1 ]        ← results collected to driver
# MAGIC ```
# MAGIC
# MAGIC > `sc` and `.rdd` are not available on serverless (Spark Connect).
# MAGIC > `mapInPandas` is the serverless-compatible way to run per-partition (per-core) logic.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 0: Parameters
# MAGIC
# MAGIC | Parameter | Default | Description |
# MAGIC |---|---|---|
# MAGIC | `num_partitions` | `8` | Number of cores to dispatch to |

# COMMAND ----------

dbutils.widgets.text("num_partitions", "8")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Discover partition count

# COMMAND ----------

num_partitions = int(dbutils.widgets.get("num_partitions"))
print(f"Dispatching to {num_partitions} cores")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Dispatch one task per core
# MAGIC
# MAGIC `spark.range(0, N, 1, N)` creates a DataFrame with exactly N partitions,
# MAGIC each holding one row whose `id` value equals the partition index.
# MAGIC
# MAGIC `mapInPandas` runs the function once per partition — i.e., once per core.
# MAGIC The `id` value passed in is used as the executor ID.

# COMMAND ----------

import pandas as pd

def work_on_core(iterator):
    for pdf in iterator:
        for executor_id in pdf["id"]:
            print(f"Executor ID: {executor_id}")
        yield pdf

df = spark.range(0, num_partitions, 1, num_partitions)
results = df.mapInPandas(work_on_core, schema="id long").collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Summary

# COMMAND ----------

print(f"Dispatched to {len(results)} cores: {sorted(r['id'] for r in results)}")
