# Worley Data Engineering Workshop — Before You Start

Welcome! Before the hands-on modules, get these three things sorted.

## 1. Find your catalog

Your facilitator has already created a Unity Catalog just for you. It looks like:

```
worley_<firstname>_<lastname>
```

For example: `worley_peter_parker`. Check the published attendee list the facilitator will share at the start (or ask them).

You are **owner** of your own catalog. You also have **read** access to every other attendee's catalog, so you can see what others are building.

## 2. The one cell you'll edit in every notebook

Each notebook in this workshop opens with a single config cell like this:

```python
# EDIT THIS — point to your own catalog
CATALOG = "worley_firstname_lastname"
SCHEMA_BRONZE = "bronze"
SCHEMA_SILVER = "silver"
SCHEMA_GOLD   = "gold"
LANDING_VOLUME = "/Volumes/workshop_shared/landing/mining_sample"
```

**Replace `worley_firstname_lastname` with your own catalog name** at the top of every notebook. That's it — all the other cells reference these constants.

The `LANDING_VOLUME` path is the **shared hub** — 175M rows of synthetic mining production data that everyone reads from (and nobody writes to).

## 3. Attach to compute

Every notebook runs on **serverless** — you don't need to pick or start a cluster. If the first cell asks for compute, pick the default serverless environment.

## Module flow (~3h total)

| # | Module |
|---|---|
| 1 | Schemas & grants |
| 2 | Bronze ingest |
| 3 | Silver transforms |
| 4 | Gold + column/row masks |
| 5 | Lineage, history, orchestration |
| 6 | Self-serve with the Databricks Assistant |
| 7 | Genie Space, dashboards, Discover |
| 8 | Extension — Knowledge Assistant Agent |

Work through them in order. Each one depends on the tables the previous one created.


