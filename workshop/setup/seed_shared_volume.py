# Databricks notebook source
# MAGIC %md
# MAGIC # Seed the Workshop Shared Landing Volume
# MAGIC
# MAGIC Run this notebook **once** before the workshop, from a user with account-admin
# MAGIC (or metastore-admin) privileges. It:
# MAGIC
# MAGIC 1. Creates `workshop_shared` catalog + `landing` schema + `mining_sample` Volume
# MAGIC 2. Generates ~175M rows of synthetic mining data into the Volume as Parquet
# MAGIC 3. Grants `USE CATALOG` + `USE SCHEMA` + `READ VOLUME` to `account users`
# MAGIC
# MAGIC Attendees' bronze ingest notebooks (Module 2) read from this Volume. They never write to it.
# MAGIC
# MAGIC **Runtime:** ~10-15 minutes on serverless.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Config — edit if you want to use different names

# COMMAND ----------

SHARED_CATALOG = "workshop_shared"
LANDING_SCHEMA = "landing"
LANDING_VOLUME = "mining_sample"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create catalog, schema, volume

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {SHARED_CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SHARED_CATALOG}.{LANDING_SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {SHARED_CATALOG}.{LANDING_SCHEMA}.{LANDING_VOLUME}")

print(f"Ready: {SHARED_CATALOG}.{LANDING_SCHEMA}.{LANDING_VOLUME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Generate data
# MAGIC
# MAGIC Reuses the generator functions from `scripts/setup.py`. If you'd rather run the
# MAGIC CLI script directly, skip this cell and run:
# MAGIC
# MAGIC `python scripts/setup.py workshop_shared landing mining_sample`

# COMMAND ----------

import sys

# Add repo root to path so we can import the generator functions
REPO_ROOT = "/Workspace/Repos/pavan.raju@databricks.com/mining-bootstrap"  # EDIT if your repo lives elsewhere
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from scripts.setup import (
    generate_mine_sites_table,
    generate_production_data,
    generate_equipment_maintenance_data,
)

volume_path = f"/Volumes/{SHARED_CATALOG}/{LANDING_SCHEMA}/{LANDING_VOLUME}"
generate_mine_sites_table(spark, f"{volume_path}/mine_sites")
generate_production_data(spark, f"{volume_path}/production")
generate_equipment_maintenance_data(spark, f"{volume_path}/equipment_maintenance")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Grant read access to all attendees

# COMMAND ----------

spark.sql(f"GRANT USE CATALOG ON CATALOG {SHARED_CATALOG} TO `account users`")
spark.sql(f"GRANT USE SCHEMA ON SCHEMA {SHARED_CATALOG}.{LANDING_SCHEMA} TO `account users`")
spark.sql(f"GRANT READ VOLUME ON VOLUME {SHARED_CATALOG}.{LANDING_SCHEMA}.{LANDING_VOLUME} TO `account users`")
spark.sql(f"GRANT SELECT ON SCHEMA {SHARED_CATALOG}.{LANDING_SCHEMA} TO `account users`")

print("Grants applied.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Verify
# MAGIC
# MAGIC Spot-check the Volume contents and row counts.

# COMMAND ----------

for sub in ("mine_sites", "production", "equipment_maintenance"):
    path = f"/Volumes/{SHARED_CATALOG}/{LANDING_SCHEMA}/{LANDING_VOLUME}/{sub}"
    n = spark.read.parquet(path).count()
    print(f"{sub:25s}  {n:>15,} rows  ({path})")
