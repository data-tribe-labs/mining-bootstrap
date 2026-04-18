# Databricks notebook source
# MAGIC %md
# MAGIC # Module 7 — Genie Space, Dashboards, Discover
# MAGIC
# MAGIC You (or someone using the Assistant in Module 6) have built
# MAGIC `gold.site_production_targets` and joined it with gold production. Now put that data
# MAGIC in front of the people who actually need it:
# MAGIC
# MAGIC 1. Ask questions in plain English via a **Genie Space**
# MAGIC 2. Build a **dashboard** to share with your team
# MAGIC 3. Let colleagues **discover** your published assets without bugging you

# COMMAND ----------

# MAGIC %md
# MAGIC ## Config — edit this cell

# COMMAND ----------

# EDIT THIS — point to your own catalog
CATALOG = "workshop_firstname_lastname"

SCHEMA_GOLD = "gold"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create a Genie Space (UI walkthrough)
# MAGIC
# MAGIC Genie lets non-technical users ask questions in natural language and get SQL +
# MAGIC answers back.
# MAGIC
# MAGIC 1. Left sidebar → **Genie**
# MAGIC 2. **New** → Give it a name like `Mining Gold — <yourname>`
# MAGIC 3. Add tables: `<your_catalog>.gold.daily_production_summary`,
# MAGIC    `<your_catalog>.gold.equipment_risk_summary`,
# MAGIC    `<your_catalog>.gold.site_production_targets`
# MAGIC 4. Add some context ("This is mining production data. Each row is a site × date.
# MAGIC    `total_tons` is daily tonnes extracted.")
# MAGIC 5. Ask questions:
# MAGIC    - "Which site produced the most tons last month?"
# MAGIC    - "Show me equipment with health score below 70"
# MAGIC    - "Which sites are missing their production target?"
# MAGIC 6. Once you're happy, **Share** → add a neighbour. They can now ask questions against
# MAGIC    your gold data — and they see the masked values because masks are enforced at the
# MAGIC    table level.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Build a Lakeview dashboard (UI walkthrough)
# MAGIC
# MAGIC 1. Left sidebar → **Dashboards** → **Create dashboard**
# MAGIC 2. Name it `Mining Gold — <yourname>`
# MAGIC 3. Add a dataset — paste this SQL:
# MAGIC
# MAGIC    ```sql
# MAGIC    SELECT production_date, SUM(total_tons) AS tons
# MAGIC    FROM <your_catalog>.gold.daily_production_summary
# MAGIC    WHERE production_date >= DATE_SUB(CURRENT_DATE(), 90)
# MAGIC    GROUP BY production_date
# MAGIC    ```
# MAGIC
# MAGIC 4. Add 3 tiles:
# MAGIC    - **Line chart** — tons by date
# MAGIC    - **Bar chart** — top 10 sites by tons (add another dataset: `SELECT site_id, SUM(total_tons) AS tons FROM ... GROUP BY site_id ORDER BY tons DESC LIMIT 10`)
# MAGIC    - **Counter** — total tons last 90 days
# MAGIC 5. **Publish** and share with your neighbour

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Databricks One / Discover (UI walkthrough)
# MAGIC
# MAGIC Your dashboards and Genie Spaces show up automatically in Databricks One / Discover
# MAGIC so business users can find them without having to know the catalog path.
# MAGIC
# MAGIC 1. Top-left workspace switcher → **Databricks One** (if available in your workspace)
# MAGIC    or **Catalog → Discover**
# MAGIC 2. Search for your dashboard or Genie Space name
# MAGIC 3. Notice it shows the **tables it depends on**, the **owner**, and **recent queries**
# MAGIC 4. Ask a neighbour to find your asset the same way
# MAGIC
# MAGIC This is how a data consumer with no platform knowledge starts their day — search,
# MAGIC find, use.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Recap
# MAGIC
# MAGIC - Genie + Dashboards turn gold tables into self-service tooling
# MAGIC - Discover makes everything findable without tribal knowledge
# MAGIC - Masks + row filters from Module 4 apply to **all** these surfaces automatically
# MAGIC
# MAGIC Next (if there's time): Module 8 — build a Knowledge Assistant Agent on your own PDFs.
