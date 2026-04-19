# Databricks notebook source
# MAGIC %md
# MAGIC # Module 5 — Lineage, History, Orchestration
# MAGIC
# MAGIC You've built a full bronze → silver → gold stack. Now let's see it from the other
# MAGIC side:
# MAGIC
# MAGIC 1. **Lineage** — Unity Catalog tracks every table's upstream + downstream automatically
# MAGIC 2. **Delta history** — every table remembers what changed, when, and by whom
# MAGIC 3. **Orchestration** — wire the three modules into a scheduled Job

# COMMAND ----------

# MAGIC %md
# MAGIC ## Config — edit this cell

# COMMAND ----------

# EDIT THIS — point to your own catalog
CATALOG = "worley_firstname_lastname"

SCHEMA_GOLD = "gold"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Delta history
# MAGIC
# MAGIC Every Delta table keeps a full operation log. Every write, every MERGE, every
# MAGIC schema change. Use it for audit, debugging, or time-travel recovery.

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {CATALOG}.{SCHEMA_GOLD}.daily_production_summary"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Time travel
# MAGIC
# MAGIC Pick a version number from the history above and query that snapshot directly.
# MAGIC This is how you answer "what did the table look like at 3pm yesterday?" without
# MAGIC building a special audit pipeline.

# COMMAND ----------

# Example: read version 0 (the very first write). Swap for any version you see above.
display(spark.sql(f"""
    SELECT COUNT(*) AS rows_at_v0
    FROM {CATALOG}.{SCHEMA_GOLD}.daily_production_summary VERSION AS OF 0
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Lineage (UI walkthrough)
# MAGIC
# MAGIC Lineage isn't something you query — it's baked into the Catalog Explorer UI.
# MAGIC
# MAGIC **Do this live:**
# MAGIC
# MAGIC 1. Left sidebar → **Catalog**
# MAGIC 2. Navigate to `<your_catalog>` → `gold` → `daily_production_summary`
# MAGIC 3. Click the **Lineage** tab
# MAGIC 4. You should see:
# MAGIC    - **Upstream:** `silver.daily_production` → `bronze.daily_production` → `workshop_shared.landing.mining_sample` volume
# MAGIC    - **Downstream:** empty for now (wait until Module 6 — once you query from Genie / a dashboard, they'll show up here)
# MAGIC 5. Click "See more" on the upstream graph — you can drill into column-level lineage
# MAGIC
# MAGIC Every table, every view, every notebook run is captured. No extra code. No extra tool.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Build a 3-task Job
# MAGIC
# MAGIC Walk through creating a Job in the UI that runs bronze → silver → gold in sequence:
# MAGIC
# MAGIC 1. Left sidebar → **Workflows** → **Create job**
# MAGIC 2. Name it `worley_<yourname>_pipeline`
# MAGIC 3. **Task 1:** name `bronze`, type **Notebook**, pick `workshop/02_bronze_ingest`, serverless compute
# MAGIC 4. **Task 2:** name `silver`, type **Notebook**, pick `workshop/03_silver_transforms`, depends on `bronze`
# MAGIC 5. **Task 3:** name `gold`, type **Notebook**, pick `workshop/04_gold_and_masks`, depends on `silver`
# MAGIC 6. **Run now** and watch the DAG fill in green as each task finishes
# MAGIC
# MAGIC In production you'd add a schedule (hourly/daily), alerting, retry policies — but
# MAGIC sequential execution with dependency tracking is the backbone.
# MAGIC
# MAGIC ### Why not just run notebooks manually?
# MAGIC
# MAGIC - **Failure handling** — tasks retry on transient errors, the job fails loudly if bronze fails
# MAGIC - **Dependencies** — silver won't start until bronze succeeds
# MAGIC - **Observability** — every run logged, every task has a duration + outcome
# MAGIC - **Scheduling** — cron + event triggers (file arrivals, table changes)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 (optional): Job config as JSON
# MAGIC
# MAGIC After you create the job in the UI, open it → three dots → **View JSON**. You'll see
# MAGIC something like the snippet below. You can check this into git, reproduce the job in
# MAGIC another workspace, or build it as a Databricks Asset Bundle.
# MAGIC These jobs also have Terraform support.

# COMMAND ----------

# MAGIC %md
# MAGIC ```json
# MAGIC {
# MAGIC   "name": "worley_<yourname>_pipeline",
# MAGIC   "tasks": [
# MAGIC     {
# MAGIC       "task_key": "bronze",
# MAGIC       "notebook_task": {"notebook_path": "/Workspace/.../workshop/02_bronze_ingest"},
# MAGIC       "environment_key": "default"
# MAGIC     },
# MAGIC     {
# MAGIC       "task_key": "silver",
# MAGIC       "depends_on": [{"task_key": "bronze"}],
# MAGIC       "notebook_task": {"notebook_path": "/Workspace/.../workshop/03_silver_transforms"},
# MAGIC       "environment_key": "default"
# MAGIC     },
# MAGIC     {
# MAGIC       "task_key": "gold",
# MAGIC       "depends_on": [{"task_key": "silver"}],
# MAGIC       "notebook_task": {"notebook_path": "/Workspace/.../workshop/04_gold_and_masks"},
# MAGIC       "environment_key": "default"
# MAGIC     }
# MAGIC   ],
# MAGIC   "environments": [
# MAGIC     {"environment_key": "default", "spec": {"client": "2"}}
# MAGIC   ]
# MAGIC }
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Recap
# MAGIC
# MAGIC - Every Delta table has versioned history — `DESCRIBE HISTORY` + `VERSION AS OF`
# MAGIC - Unity Catalog tracks lineage for free in the Catalog Explorer
# MAGIC - Jobs orchestrate sequential + parallel tasks with retries, schedules, alerts
# MAGIC
# MAGIC Next: Module 6 — hand the gold tables over to the analyst / business-user persona.
