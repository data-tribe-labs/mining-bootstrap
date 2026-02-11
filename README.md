# Mining Bootstrap – Spark Declarative Pipelines

This project runs a **Spark Declarative Pipelines (SDP) (previously known as Delta Live Tables)** pipeline with Databricks Asset Bundles (DAB). It’s set up for customers new to Databricks: you only need to configure a few values, then deploy and run.

---

## 1. Set up the Databricks CLI

### Install the CLI

**Option A – pip (Python)**

```bash
pip install databricks-cli
```

**Option B – Homebrew (macOS)**

```bash
brew install databricks-cli
```

**Option C – Official installers**

See [Install the Databricks CLI](https://docs.databricks.com/en/dev-tools/cli/index.html).

Check that it works:

```bash
databricks --version
```

### Configure authentication

1. **Get your workspace URL and a token**
   - In Databricks: **Settings** (gear icon) → **Developer** → **Access tokens**.
   - Create a token and copy it.
   - Your workspace URL looks like: `https://<workspace-name>.cloud.databricks.com` (or your organization’s host).

2. **Configure the CLI**

   ```bash
   databricks configure --token
   ```

   When prompted:

   - **Databricks Host:** `https://<your-workspace>.cloud.databricks.com`
   - **Token:** paste the token you created

   This writes a profile to `~/.databrickscfg`. You can also use [OAuth (e.g. Azure)](https://docs.databricks.com/en/dev-tools/cli/auth.html) if your org uses it.

3. **If you have multiple workspaces**

   Use a named profile and point the bundle at it:

   ```bash
   databricks configure --token --profile mycompany
   ```

   Then when running bundle commands (see below), use:

   ```bash
   export DATABRICKS_CONFIG_PROFILE=mycompany
   databricks bundle deploy -t dev
   ```

   Or set the profile in the bundle’s target (see “Fields to update” below).

---

## 2. What you need to change for your environment

All customer-specific values are in **variables** and **targets**. You can change them in `databricks.yml` and, for notifications/tags, in `resources/resources.yml`.

### In `databricks.yml`

| What to set | Where | Description |
|-------------|--------|-------------|
| **Workspace host** | `targets.dev.workspace.host` and `targets.prod.workspace.host` | Your Databricks workspace URL, e.g. `https://acme.cloud.databricks.com`. |
| **Catalog** | `targets.dev.variables.catalog` and `targets.prod.variables.catalog` | Unity Catalog catalog where the pipeline will create tables (e.g. `acme_dev`, `acme_prod`). You must have `USE CATALOG` and `CREATE SCHEMA` (or existing `bronze` / `silver` schemas) there. |
| **Prod run-as user** | `targets.prod.run_as.user_name` | Databricks user that runs the pipeline in prod (e.g. a service principal or CI user). You can use your own email while you're still learning. |

Example for a customer “acme” with dev and prod:

```yaml
# databricks.yml (excerpt)

targets:
  # Development environment
  dev:
    mode: development
    default: true
    workspace:
      host: https://acme.cloud.databricks.com # change this to your workspace host
    
    variables:
      catalog: testuser_dev # replace this with your catalog name
      volume: mining_sample
      production_full_path: '/Volumes/${var.catalog}/${var.schema_landing}/${var.volume}/production'
      equipment_maintenance_full_path: '/Volumes/${var.catalog}/${var.schema_landing}/${var.volume}/equipment_maintenance'
      mine_site_full_path: '/Volumes/${var.catalog}/${var.schema_landing}/${var.volume}/mine_sites'
  
# Ignore this for now. It's for production environment.
  prod:
    mode: production
    workspace:
      host: https://acme.cloud.databricks.com # change this to your workspace host
    
    variables:
      catalog: testuser_dev # replace this with your catalog name
    
    run_as:
      user_name: testuser@databricks.com # replace this with your own email
```

### In `resources/resources.yml`

| What to set | Where | Description |
|-------------|--------|-------------|
| **Notification emails** | `resources.pipelines.mining_bootstrap.notifications[0].email_recipients` | Replace with your team’s emails for pipeline success/failure. |

No need to change pipeline `catalog` / `schema` in `resources/resources.yml` if you already set them via variables in `databricks.yml` (they use `${var.catalog}` and `${var.schema_bronze}`).



---

## 3. Deploy and run

From the project root (where `databricks.yml` lives):

**Run the setup_base_datasets** pipeline before 

```bash
# Validate configuration (no deploy)
databricks bundle validate --target dev

# Deploy the pipeline to the dev target.
# You can check if the pipeline has been deployed through the UI.
databricks bundle deploy --target dev

# (Optional) Start a pipeline run.
# You can also trigger the pipeline through the UI
databricks bundle run setup_base_datasets --target dev


databricks bundle run mining_bootstrap --target dev
```

Use `-t prod` (--target) for production once you’ve set `targets.prod` and (if required) `DATABRICKS_CONFIG_PROFILE`.

---  

# Extended Documentation 

## 1. Overriding variables from the CLI

You can override any bundle variable when you run a command, without editing YAML.

### Option 1: `--var` on the command line

Use one or more `--var="name=value"`:

```bash
# Override catalog for this deploy
databricks bundle deploy -t dev --var="catalog=my_catalog"

# Override multiple variables
databricks bundle deploy -t dev --var="catalog=acme_dev" --var="volume=my_volume"

# Same for validate or run
databricks bundle validate -t dev --var="catalog=acme_dev"
databricks bundle run mining_bootstrap -t dev --var="catalog=acme_dev"
```

So **yes: you can pass a variable (e.g. catalog) into the CLI and it overrides the value from `databricks.yml`** (and from `variable-overrides.json` for that target).

### Option 2: environment variables

Any variable can be overridden with an env var named `BUNDLE_VAR_<variable_name>`:

```bash
export BUNDLE_VAR_catalog=acme_dev
export BUNDLE_VAR_volume=mining_sample
databricks bundle deploy -t dev
```

Useful in scripts or CI/CD where you don’t want to put secrets or environment-specific values in the repo.

### Precedence (highest wins)

1. `--var` on the command line  
2. `BUNDLE_VAR_*` environment variables  
3. `.databricks/bundle/<target>/variable-overrides.json` (if present)  
4. `targets.<target>.variables` in `databricks.yml`  
5. `variables.<name>.default` in `databricks.yml`  

So a CLI override like `--var="catalog=my_catalog"` wins over what’s in `resources` or `databricks.yml` for that run.

---

## Quick reference – where things live

| Goal | File / place |
|------|-------------------------------|
| Workspace URL, catalog, schemas, volume | `databricks.yml` → `variables` and `targets.<target>.variables` |
| Pipeline notifications, tags | `resources/resources.yml` → `resources.pipelines.mining_bootstrap` |
| Setup-base-datasets job (notebook, params) | `resources/setup_job.yml` and `scripts/setup_data.ipynb` |
| Override catalog (or any variable) for one run | CLI: `--var="catalog=my_catalog"` or env: `BUNDLE_VAR_catalog=my_catalog` |
| Auth / profile | `~/.databrickscfg` or `DATABRICKS_CONFIG_PROFILE` |
