# Databricks notebook source
# MAGIC %md
# MAGIC # Module 6 — Self-serve data with the Databricks Assistant
# MAGIC
# MAGIC Put yourself in the shoes of a business user. You don't write PySpark. You don't
# MAGIC remember SQL syntax. But you have an Excel file of monthly production targets on
# MAGIC your laptop and you want to know which sites are hitting them.
# MAGIC
# MAGIC In this module you'll let the **Databricks Assistant** do the typing. You describe
# MAGIC what you want; it writes the code; you review and run it.
# MAGIC
# MAGIC ## What is the Databricks Assistant?
# MAGIC
# MAGIC The sparkle button at the top-right of any notebook cell (or `Cmd+I` / `Ctrl+I`).
# MAGIC It's an in-notebook AI helper that:
# MAGIC - **Sees your Unity Catalog schemas** — so when you ask it about `daily_production_summary`, it already knows the columns and types
# MAGIC - **Knows which catalog / schema you're in** — no need to repeat yourself
# MAGIC - **Writes PySpark, SQL, or Python** — whichever you ask for
# MAGIC - **Can explain, fix, or optimise** code you already have
# MAGIC
# MAGIC Think of it as pair-programming with someone who has read every Databricks doc
# MAGIC and every table in your workspace.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Config — edit this cell

# COMMAND ----------

# EDIT THIS — point to your own catalog
CATALOG = "worley_firstname_lastname"

SCHEMA_GOLD = "gold"

# Volume where we'll stash uploaded files (ad-hoc files live here, not in landing)
USER_VOLUME_NAME = "uploads"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create a Volume for your own uploads
# MAGIC
# MAGIC Volumes are UC-governed file storage — the right home for "I have a CSV / Excel /
# MAGIC image and I want it available in Databricks".

# COMMAND ----------

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA_GOLD}.{USER_VOLUME_NAME}")
upload_path = f"/Volumes/{CATALOG}/{SCHEMA_GOLD}/{USER_VOLUME_NAME}"
print(f"Upload files to:  {upload_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Upload the sample Excel
# MAGIC
# MAGIC The facilitator has shared a file called **`site_production_targets.xlsx`**. Grab
# MAGIC it and upload it to your Volume:
# MAGIC
# MAGIC 1. Left sidebar → **Catalog**
# MAGIC 2. `<your_catalog>` → `gold` → `uploads` volume
# MAGIC 3. Click **Upload to this volume** and drop the Excel file in

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Ask the Assistant to read the Excel and register it as a table
# MAGIC
# MAGIC **Click the next cell (the empty one), press `Cmd+I` (Mac) or `Ctrl+I` (Windows) to
# MAGIC open the Assistant, and paste this prompt:**
# MAGIC
# MAGIC > Read the Excel file at `/Volumes/<your_catalog>/gold/uploads/site_production_targets.xlsx`
# MAGIC > using pandas, then create a Spark DataFrame from it, and save it as a Delta table
# MAGIC > called `<your_catalog>.gold.site_production_targets`. Overwrite if it already exists.
# MAGIC > Print the row count at the end.
# MAGIC
# MAGIC (Replace `<your_catalog>` with your actual catalog name — the Assistant will pick up
# MAGIC the rest from context.)
# MAGIC
# MAGIC Review what the Assistant generates. If it looks reasonable, accept it and run the
# MAGIC cell. If not, ask it to fix whatever's wrong.

# COMMAND ----------

# ✨ Use the Assistant here. Suggested prompt is in the markdown cell above.


# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Peek at the new table
# MAGIC
# MAGIC Verify the upload worked before we try to join it with anything.

# COMMAND ----------

display(spark.table(f"{CATALOG}.{SCHEMA_GOLD}.site_production_targets"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Ask the Assistant to join targets with gold production
# MAGIC
# MAGIC Now the interesting part. You have:
# MAGIC - `gold.daily_production_summary` — actual production, one row per site × date
# MAGIC - `gold.site_production_targets` — monthly targets per site, which you just uploaded
# MAGIC
# MAGIC You want to know: *which sites are beating or missing their monthly target?*
# MAGIC
# MAGIC **Open the Assistant on the next cell (`Cmd+I` / `Ctrl+I`) and try this prompt:**
# MAGIC
# MAGIC > Using `<your_catalog>.gold.daily_production_summary` and
# MAGIC > `<your_catalog>.gold.site_production_targets`, write a SQL query that shows, for each
# MAGIC > site and month in the last 90 days: the actual total tons, the monthly target tons,
# MAGIC > and the delta. Order by delta descending. Limit 20. Wrap it in `display(spark.sql(...))`.
# MAGIC
# MAGIC Notice how the Assistant already knows the column names of both tables without you
# MAGIC telling it. That's the Unity Catalog context doing its work.

# COMMAND ----------

# ✨ Use the Assistant here. Suggested prompt is in the markdown cell above.


# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Try some Assistant follow-ups
# MAGIC
# MAGIC Once you've got a working query in Step 5, try these follow-up prompts on the same
# MAGIC cell (the Assistant remembers what's already there):
# MAGIC
# MAGIC - **"Explain what this query does, line by line"** — great for handing code off to
# MAGIC   a colleague
# MAGIC - **"Add a column that shows the percent of target achieved"** — small edit, watch
# MAGIC   the Assistant modify in place
# MAGIC - **"Rewrite this using PySpark DataFrame API instead of SQL"** — see the same
# MAGIC   logic in a different syntax
# MAGIC - **"Only include sites where site_status = 'Active'"** — add a filter. If you
# MAGIC   applied the row filter from Module 4, the Assistant will honour it automatically
# MAGIC   because masks are enforced at the table level.
# MAGIC
# MAGIC Each one is a one-line prompt. No copying, no syntax lookups.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Recap
# MAGIC
# MAGIC - Volumes are the bridge between "files on my laptop" and "data in Databricks"
# MAGIC - The Databricks Assistant turns natural-language intent into runnable code,
# MAGIC   UC-aware out of the box
# MAGIC - Analysts who don't know PySpark can still self-serve: upload, describe, review,
# MAGIC   run
# MAGIC
# MAGIC You now have `site_production_targets` in your gold schema. In the next module
# MAGIC (**Module 7**) you'll expose it via a Genie Space + dashboard so non-technical
# MAGIC colleagues can ask questions against it.
