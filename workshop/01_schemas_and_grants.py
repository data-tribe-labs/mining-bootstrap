# Databricks notebook source
# MAGIC %md
# MAGIC # Module 1 — Schemas & Grants
# MAGIC
# MAGIC Your catalog has already been provisioned for you. In this module you will:
# MAGIC
# MAGIC 1. Create three schemas inside your catalog: `bronze`, `silver`, `gold`
# MAGIC 2. Create a metadata Volume used by streaming checkpoints in Module 2
# MAGIC 3. Grant `SELECT` on a table to a neighbour and revoke it
# MAGIC 4. Inspect your grants with `SHOW GRANTS`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Config — edit this cell

# COMMAND ----------

# EDIT THIS — point to your own catalog
CATALOG = "worley_firstname_lastname"

SCHEMA_BRONZE = "bronze"
SCHEMA_SILVER = "silver"
SCHEMA_GOLD   = "gold"

# Pick a neighbour to share a table with (any other attendee's email)
NEIGHBOUR_EMAIL = "neighbour@worley.com"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create schemas
# MAGIC
# MAGIC Schemas are the second level of the three-level Unity Catalog namespace:
# MAGIC `catalog.schema.table`. We'll keep bronze/silver/gold separate so lineage is obvious.

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA_BRONZE}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA_SILVER}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA_GOLD}")

display(spark.sql(f"SHOW SCHEMAS IN {CATALOG}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create a metadata Volume
# MAGIC
# MAGIC Auto Loader needs somewhere to store its schema-tracking and checkpoint files.
# MAGIC We'll put those in a Volume under the bronze schema — a file-system-style storage
# MAGIC location governed by Unity Catalog.

# COMMAND ----------

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA_BRONZE}.metadata")

display(spark.sql(f"SHOW VOLUMES IN {CATALOG}.{SCHEMA_BRONZE}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create a throwaway table for the grants demo
# MAGIC
# MAGIC We need something to grant access on. Later modules build the real tables — this
# MAGIC is just a 3-row toy table to practice `GRANT` / `REVOKE` / `SHOW GRANTS`.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA_GOLD}.grants_demo (
    id INT,
    note STRING
)
""")

spark.sql(f"""
INSERT INTO {CATALOG}.{SCHEMA_GOLD}.grants_demo VALUES
    (1, 'first'), (2, 'second'), (3, 'third')
""")

display(spark.table(f"{CATALOG}.{SCHEMA_GOLD}.grants_demo"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Grant SELECT to a neighbour
# MAGIC
# MAGIC UC grants need both `USE CATALOG` / `USE SCHEMA` (to see the namespace) and
# MAGIC `SELECT` (to read the data). `provision_attendees.py` already gave every attendee
# MAGIC `USE CATALOG` + `USE SCHEMA` on your catalog, so all we need here is `SELECT`.

# COMMAND ----------

spark.sql(f"""
GRANT SELECT ON TABLE {CATALOG}.{SCHEMA_GOLD}.grants_demo
TO `{NEIGHBOUR_EMAIL}`
""")

display(spark.sql(f"SHOW GRANTS ON TABLE {CATALOG}.{SCHEMA_GOLD}.grants_demo"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Have your neighbour read it
# MAGIC
# MAGIC Tell your neighbour your catalog name. In their own notebook they should run:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT * FROM <your_catalog>.gold.grants_demo
# MAGIC ```
# MAGIC
# MAGIC They should see your 3 rows. Before the grant, that query would have errored.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Revoke

# COMMAND ----------

spark.sql(f"""
REVOKE SELECT ON TABLE {CATALOG}.{SCHEMA_GOLD}.grants_demo
FROM `{NEIGHBOUR_EMAIL}`
""")

display(spark.sql(f"SHOW GRANTS ON TABLE {CATALOG}.{SCHEMA_GOLD}.grants_demo"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Recap
# MAGIC
# MAGIC You now have:
# MAGIC - `bronze`, `silver`, `gold` schemas in your catalog
# MAGIC - A `bronze.metadata` Volume for streaming state
# MAGIC - First-hand experience granting + revoking access
# MAGIC
# MAGIC On to Module 2 — bronze ingest from the shared landing Volume.
