# Databricks notebook source
# MAGIC %md
# MAGIC # Deprovision Attendee Catalogs
# MAGIC
# MAGIC **Destructive — drops every attendee catalog and everything inside.**
# MAGIC Run this AFTER the workshop, once attendees have exported anything they want to keep.
# MAGIC
# MAGIC Guarded behind a confirmation flag — Step 3 will assert until you flip it.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Attendee list — match what you used for provisioning

# COMMAND ----------

PREFIX = "worley"

ATTENDEES = [
    # {"first_name": "Peter",  "last_name": "Parker", "email": "peter.parker@worley.com"},
    # {"first_name": "Jane",   "last_name": "Doe",    "email": "jane.doe@worley.com"},
    # ... up to 10 rows
]

assert ATTENDEES, "Fill in the ATTENDEES list before running the rest of the notebook."

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Confirm — flip this to `True` to authorise the drop

# COMMAND ----------

I_UNDERSTAND_THIS_DROPS_EVERYTHING = False

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Derive catalog names

# COMMAND ----------

import re

def slugify(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")

def catalog_for(att: dict) -> str:
    return f"{PREFIX}_{slugify(att['first_name'])}_{slugify(att['last_name'])}"

catalogs = [catalog_for(att) for att in ATTENDEES]

print("About to DROP these catalogs (cascading into all tables, volumes, and grants):")
for cat in catalogs:
    print(f"  - {cat}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Drop catalogs

# COMMAND ----------

assert I_UNDERSTAND_THIS_DROPS_EVERYTHING, (
    "Set I_UNDERSTAND_THIS_DROPS_EVERYTHING = True in Step 2 before running this cell."
)

for cat in catalogs:
    print(f"[{cat}]  DROP CASCADE")
    spark.sql(f"DROP CATALOG IF EXISTS `{cat}` CASCADE")

print("\nDone.")
