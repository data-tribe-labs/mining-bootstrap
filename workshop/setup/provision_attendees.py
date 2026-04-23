# Databricks notebook source
# MAGIC %md
# MAGIC # Provision Attendee Catalogs
# MAGIC
# MAGIC Run this notebook **once** before the workshop, from a user with metastore-admin
# MAGIC privileges.
# MAGIC
# MAGIC For every attendee in the list below it will:
# MAGIC 1. Create a Unity Catalog named `{prefix}_{first_name}_{last_name}`
# MAGIC 2. Make the attendee **owner** of their catalog
# MAGIC 3. Grant every **other** attendee read access (`USE CATALOG`, `USE SCHEMA`,
# MAGIC    `SELECT`, `EXECUTE`, `READ VOLUME`) so they can inspect peers' work
# MAGIC
# MAGIC Idempotent — safe to re-run (uses `IF NOT EXISTS`; grants are additive).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Attendee list — edit this cell

# COMMAND ----------

PREFIX = "workshop"

ATTENDEES = [
    # {"first_name": "Peter",  "last_name": "Parker", "email": "peter.parker@example.com"},
    # {"first_name": "Jane",   "last_name": "Doe",    "email": "jane.doe@example.com"},
    # ... up to 10 rows
]

assert ATTENDEES, "Fill in the ATTENDEES list before running the rest of the notebook."

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Derive sanitized catalog names
# MAGIC
# MAGIC `{prefix}_{first_name}_{last_name}` lowercased with non-alphanumerics collapsed
# MAGIC to `_`. Example: `"John O'Brien"` → `workshop_john_o_brien`.

# COMMAND ----------

import re

def slugify(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")

def catalog_for(att: dict) -> str:
    return f"{PREFIX}_{slugify(att['first_name'])}_{slugify(att['last_name'])}"

print(f"{'Catalog':40s}  Owner email")
print("-" * 80)
for att in ATTENDEES:
    print(f"{catalog_for(att):40s}  {att['email']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Create catalogs and set owners

# COMMAND ----------

for att in ATTENDEES:
    cat = catalog_for(att)
    print(f"[{cat}]  create + owner = {att['email']}")
    spark.sql(f"CREATE CATALOG IF NOT EXISTS `{cat}`")
    spark.sql(f"ALTER CATALOG `{cat}` OWNER TO `{att['email']}`")

print("\nCatalogs ready.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Cross-grant read access between attendees
# MAGIC
# MAGIC For every pair `(A, B)` where `A != B`, `B` gets read on `A`'s catalog.

# COMMAND ----------

for owner in ATTENDEES:
    owner_cat = catalog_for(owner)
    for other in ATTENDEES:
        if other["email"] == owner["email"]:
            continue
        spark.sql(f"""
            GRANT USE CATALOG, USE SCHEMA, SELECT, EXECUTE, READ VOLUME
            ON CATALOG `{owner_cat}` TO `{other['email']}`
        """)
    print(f"[{owner_cat}]  granted read to {len(ATTENDEES) - 1} peers")

print("\nCross-grants applied.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Summary to share with attendees
# MAGIC
# MAGIC Paste this into the workshop chat / slide / email so everyone knows their catalog name.

# COMMAND ----------

print(f"{'Attendee':30s} {'Email':40s} Catalog")
print("-" * 110)
for att in ATTENDEES:
    name = f"{att['first_name']} {att['last_name']}"
    print(f"{name:30s} {att['email']:40s} {catalog_for(att)}")
