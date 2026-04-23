# Databricks notebook source
# MAGIC %md
# MAGIC # Module 8 (Extension) — Knowledge Assistant Agent
# MAGIC
# MAGIC Bring your own PDFs, Word docs, or text files. We'll:
# MAGIC
# MAGIC 1. Upload them to a Volume
# MAGIC 2. Parse with the built-in `ai_parse_document` function
# MAGIC 3. Chunk into rows + embed with a Vector Search index
# MAGIC 4. Spin up an agent in the AI Playground that answers questions over your docs
# MAGIC
# MAGIC If you run out of time, take it home — everything here works on your own
# MAGIC time just as well.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Config — edit this cell

# COMMAND ----------

# EDIT THIS — point to your own catalog
CATALOG = "workshop_firstname_lastname"

# We'll put everything agent-related in its own schema
SCHEMA_AGENT = "agent"

# Volume where you'll drop PDFs / text files
DOCS_VOLUME = "knowledge_docs"

# Vector Search endpoint — use the default shared one, or ask your facilitator for the workshop endpoint name
VS_ENDPOINT = "workshop_vs_endpoint"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Schema + Volume for documents

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA_AGENT}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA_AGENT}.{DOCS_VOLUME}")

docs_path = f"/Volumes/{CATALOG}/{SCHEMA_AGENT}/{DOCS_VOLUME}"
print(f"Drop your files at:  {docs_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Upload your files
# MAGIC
# MAGIC Use the UI:
# MAGIC
# MAGIC 1. Left sidebar → **Catalog** → `<your_catalog>` → `agent` → `knowledge_docs`
# MAGIC 2. **Upload to this volume** — drop in 2-5 PDFs or text files
# MAGIC 3. Come back here and run the cell below to confirm

# COMMAND ----------

import os

files = os.listdir(docs_path)
print(f"{len(files)} files:")
for f in files:
    size_kb = os.path.getsize(f"{docs_path}/{f}") / 1024
    print(f"  {f}  ({size_kb:,.1f} KB)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Parse the documents into text
# MAGIC
# MAGIC `ai_parse_document` is a Databricks SQL AI function that turns a binary file
# MAGIC (PDF, Word, etc.) into structured text. We run it over every file in the Volume.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA_AGENT}.documents_raw AS
SELECT
    path,
    ai_parse_document(content) AS parsed
FROM READ_FILES('{docs_path}', format => 'binaryFile')
""")

display(spark.sql(f"""
    SELECT path, LENGTH(parsed:document:pages) AS page_count
    FROM {CATALOG}.{SCHEMA_AGENT}.documents_raw
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Chunk into searchable rows
# MAGIC
# MAGIC Each doc is split into ~500-token chunks so we can retrieve relevant passages
# MAGIC instead of the entire document when the agent answers a question.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA_AGENT}.document_chunks (
    chunk_id STRING,
    path STRING,
    content STRING
)
TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")

spark.sql(f"""
INSERT INTO {CATALOG}.{SCHEMA_AGENT}.document_chunks
SELECT
    uuid() AS chunk_id,
    path,
    chunk.content AS content
FROM (
    SELECT
        path,
        EXPLODE(ai_chunk(CAST(parsed:document:pages AS STRING), 500, 100)) AS chunk
    FROM {CATALOG}.{SCHEMA_AGENT}.documents_raw
)
""")

display(spark.sql(f"""
    SELECT COUNT(*) AS chunks, COUNT(DISTINCT path) AS docs
    FROM {CATALOG}.{SCHEMA_AGENT}.document_chunks
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Build a Vector Search index
# MAGIC
# MAGIC Vector Search keeps an embedding of each chunk and serves sub-second similarity
# MAGIC queries. We use the managed embedding model so we don't have to compute embeddings
# MAGIC ourselves — the index handles it from the `content` column.

# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient

vsc = VectorSearchClient()

index_name = f"{CATALOG}.{SCHEMA_AGENT}.document_chunks_idx"
source_table = f"{CATALOG}.{SCHEMA_AGENT}.document_chunks"

vsc.create_delta_sync_index(
    endpoint_name=VS_ENDPOINT,
    index_name=index_name,
    source_table_name=source_table,
    pipeline_type="TRIGGERED",
    primary_key="chunk_id",
    embedding_source_column="content",
    embedding_model_endpoint_name="databricks-gte-large-en",
)

print(f"Index creating: {index_name}")
print("Refresh the Catalog UI in 1-2 min to see it ready.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Test the index

# COMMAND ----------

index = vsc.get_index(endpoint_name=VS_ENDPOINT, index_name=index_name)

results = index.similarity_search(
    query_text="What is the main topic covered?",
    columns=["path", "content"],
    num_results=3,
)
display(results)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Build the agent in AI Playground
# MAGIC
# MAGIC 1. Left sidebar → **Playground**
# MAGIC 2. Pick a Foundation Model (Claude, Llama 3.1, etc.)
# MAGIC 3. **Tools** → add your Vector Search index:
# MAGIC    `<your_catalog>.agent.document_chunks_idx`
# MAGIC 4. **System prompt**:
# MAGIC    > You are a knowledge assistant. Use the search tool to find relevant passages
# MAGIC    > in the user's documents before answering. Cite the `path` of any document you
# MAGIC    > reference.
# MAGIC 5. Ask a question that only your documents can answer
# MAGIC 6. Once happy → **Export → Agent** to turn it into a deployed Mosaic AI Agent
# MAGIC    endpoint you can call from apps / dashboards / Genie

# COMMAND ----------

# MAGIC %md
# MAGIC ## Recap
# MAGIC
# MAGIC - Volumes hold arbitrary files, governed by UC
# MAGIC - `ai_parse_document` + `ai_chunk` turn binaries into searchable text
# MAGIC - Vector Search indexes the chunks with zero embedding code
# MAGIC - AI Playground is the fastest path from "I have data" to "I have an agent"

