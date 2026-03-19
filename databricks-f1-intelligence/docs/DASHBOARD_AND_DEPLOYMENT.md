# Dashboard & Deployment — F1 Intelligence

This document covers two things: how the Streamlit dashboard is structured and how it connects to the data, and how the Databricks Asset Bundle (DAB) handles deployment of both the ETL pipeline and the app.

---

## Streamlit Dashboard

### Structure

The app follows Streamlit's multi-page convention. `app.py` is the entry point — it sets up the shared database connection and helper functions that every page imports. The four pages live under `streamlit_app/pages/` and are numbered so Streamlit renders them in the correct sidebar order.

```
streamlit_app/
├── app.py                      # Entry point, shared DB connection + query helpers
├── app.yaml                    # Databricks Apps config (command, env vars)
├── requirements.txt            # Dependencies installed by Databricks Apps at startup
└── pages/
    ├── 01_championship.py      # Driver & constructor standings, points progression chart
    ├── 02_race_results.py      # Race results table, qualifying, fastest lap callout
    ├── 03_tyre_strategy.py     # Compound usage, stint breakdown, lap-level timeline
    └── 04_circuit_benchmarks.py # Fastest laps and pole times by circuit
```

### Shared Connection Layer (`app.py`)

Rather than each page managing its own database connection, I centralise everything in `app.py`. Pages import two helpers from it: `run_query()` and `table()`.

```python
@st.cache_resource
def get_connection():
    return dbsql.connect(
        server_hostname=HOSTNAME,
        http_path=HTTP_PATH,
        access_token=TOKEN,
    )

def run_query(sql: str) -> list[dict]:
    conn = get_connection()
    with conn.cursor() as cursor:
        cursor.execute(sql)
        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]

def table(name: str) -> str:
    return f"{CATALOG}.{SCHEMA}.{name}"
```

`@st.cache_resource` means the connection is created once per app process and reused across all page renders — no reconnect overhead on every query. The `table()` helper injects the catalog and schema so pages never hardcode the namespace; switching from `f1_dev` to `f1_prod` is a single env var change.

Connection credentials are read from environment variables: `DATABRICKS_HOST` (or `DATABRICKS_SERVER_HOSTNAME`), `DATABRICKS_HTTP_PATH`, and `DATABRICKS_TOKEN`. Inside Databricks Apps, `DATABRICKS_HOST` and `DATABRICKS_TOKEN` are auto-injected by the runtime using the app's service principal — no manual secrets management needed. For local development, I export them manually before running `streamlit run app.py`.

One macOS-specific quirk: the system keychain can contain conflicting CA certificates that cause the `databricks-sql-connector` to hang indefinitely on TLS negotiation. I work around this by forcing `requests` and `urllib3` to use `certifi`'s clean CA bundle at startup:

```python
os.environ.setdefault("REQUESTS_CA_BUNDLE", certifi.where())
os.environ.setdefault("SSL_CERT_FILE", certifi.where())
```

`setdefault` is used so this only applies if the env vars aren't already set — Databricks Apps on Linux is unaffected.

### Pages

#### Championship (`01_championship.py`)
Queries `gold_driver_championship` for the current standings table and `silver_driver_standings` for the per-round points progression chart. I intentionally use two different tables here: Gold holds the current state (one row per driver), while Silver holds the full round-by-round history needed for the line chart. A season selector in the sidebar drives both.

Also renders constructor standings from `gold_constructor_championship` as a bar chart underneath.

#### Race Results (`02_race_results.py`)
Queries `silver_race_results` for the classification table and `silver_qualifying` for the qualifying results below it. Podium rows are highlighted gold/silver/bronze using Pandas `.style.apply()`. A fastest lap callout is shown as a `st.success()` banner using the `fastest_lap_rank = 1` filter. Sprint weekends that have no qualifying data show an informational message rather than an error.

Sidebar has two cascading selectors: season first, then race name (which maps to a round number).

#### Tyre Strategy (`03_tyre_strategy.py`)
This page uses data from two sources in the same view:
- `gold_tyre_strategy_report` for the per-driver stint summary table and compound distribution pie chart
- `silver_lap_analysis` for the lap-by-lap compound scatter plot at the bottom

The scatter plot shows each lap as a coloured square — compound colours match the standard F1 colour scheme (red = Soft, yellow = Medium, grey = Hard, green = Intermediate, blue = Wet). This visual is only rendered if lap telemetry data is available for the selected race.

#### Circuit Benchmarks (`04_circuit_benchmarks.py`)
Queries `gold_circuit_benchmarks` directly — no joins needed since that table is already fully denormalised at Gold. Renders a searchable table plus two bar charts: all-time fastest race lap per circuit and all-time pole time per circuit, both coloured by driver. This page has no sidebar selectors because the benchmarks are cross-season by definition.

---

## Databricks Asset Bundle (DAB)

### What DAB does

Databricks Asset Bundles let me define all pipeline infrastructure — notebooks, jobs, apps, and their parameters — as code in `databricks.yml` and `resources/*.yml`. Running `make deploy-dev` uploads everything to the workspace and registers the job and app in one command, with no manual clicking in the UI.

### `databricks.yml`

The root config file ties everything together:

```yaml
bundle:
  name: f1-intelligence

include:
  - resources/*.yml       # pulls in the job definition

artifacts:
  python_artifact:
    type: whl
    build: uv build --wheel  # builds utils/ into a wheel before deploying

resources:
  apps:
    f1_intelligence_app:
      name: "f1-intelligence-app"
      source_code_path: streamlit_app/

variables:
  catalog:
    default: f1_intelligence
  schema:
    default: f1_dev

targets:
  dev:
    mode: development       # prefixes all resources with [dev username]
    default: true
    workspace:
      host: https://your-workspace.cloud.databricks.com
    variables:
      schema: f1_dev

  prod:
    mode: production
    workspace:
      root_path: /Workspace/Users/.../.bundle/f1-intelligence/prod
    variables:
      schema: f1_prod
    permissions:
      - user_name: your-email@example.com
        level: CAN_MANAGE
```

The `artifacts` section is important — `uv build --wheel` compiles `utils/` into a Python wheel before each deploy. The notebooks then install and import this wheel rather than needing the source files to be on the cluster's Python path. This is cleaner than syncing loose `.py` files and ensures the cluster always runs the exact version that was deployed.

The `variables` block means catalog and schema are single points of control. I never hardcode `f1_dev` inside a notebook — everything reads from the widget that DAB populates from the job parameter, which in turn reads from `${var.schema}`.

### `resources/f1_intelligence_job.job.yml`

The job chains three notebook tasks with strict dependency ordering:

```
bronze_ingestion → silver_transformation → gold_aggregation
```

If Bronze fails, Silver and Gold don't run. This is intentional — there's no point processing Silver if the underlying Bronze rows haven't been updated yet.

All three tasks share the same set of parameters defined at the job level:

| Parameter | Default | Purpose |
|---|---|---|
| `catalog` | `f1_intelligence` | Unity Catalog catalog name |
| `schema` | `f1_dev` | Schema (switches to `f1_prod` in prod target) |
| `season` | `2024` | Which season to process |
| `round` | `all` | Specific round or `all` |
| `mode` | `incremental` | `full` for first load, `incremental` for subsequent runs |

Parameters flow from the job level into each notebook via `base_parameters`. The `catalog` and `schema` parameters are bound to the DAB variables, so deploying to the prod target automatically wires `f1_prod` into every notebook without changing any notebook code.

The `environments` block installs the built wheel on the cluster before notebooks run:

```yaml
environments:
  - environment_key: default
    spec:
      environment_version: "4"
      dependencies:
        - ../dist/*.whl
```

### Dev vs Prod targets

The `dev` target uses `mode: development`, which prefixes every resource name with `[dev your_username]`. This means the job shows up as `[dev pravbala] F1 Intelligence ETL` and the app as `[dev pravbala] f1-intelligence-app`. Multiple people can deploy their own dev environments in the same workspace without stepping on each other.

The `prod` target uses `mode: production` and a fixed `root_path`, so deployed artifacts land at a predictable workspace path. The `permissions` block ensures only the designated service account has `CAN_MANAGE`.

### Deploying the app

The Streamlit app is declared as a DAB `apps` resource:

```yaml
resources:
  apps:
    f1_intelligence_app:
      name: "f1-intelligence-app"
      source_code_path: streamlit_app/
```

`make deploy-dev` uploads the `streamlit_app/` directory to the workspace and registers the app with Databricks Apps. The app then reads its startup command and environment from `streamlit_app/app.yaml`:

```yaml
command: ["streamlit", "run", "app.py", "--server.port", "8080", "--server.address", "0.0.0.0"]

env:
  - name: CATALOG
    value: f1_intelligence
  - name: SCHEMA
    value: f1_dev
  - name: DATABRICKS_HTTP_PATH
    value: /sql/1.0/warehouses/your-warehouse-id
```

`DATABRICKS_HOST` and `DATABRICKS_TOKEN` are auto-injected by the Databricks Apps runtime using the app's service principal — they don't need to appear in `app.yaml`. The service principal needs `CAN_USE` on the SQL warehouse and `SELECT` on the Unity Catalog tables it queries.

### Running locally

```bash
cd streamlit_app
pip install -r requirements.txt
export DATABRICKS_SERVER_HOSTNAME=your-workspace.cloud.databricks.com
export DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/your-warehouse-id
export DATABRICKS_TOKEN=your-pat-token
export CATALOG=f1_intelligence
export SCHEMA=f1_dev
streamlit run app.py
```

The app connects to the same Gold tables in the workspace — it doesn't need a local Spark session or any local data files. This makes local development straightforward: run the pipeline on Databricks once, then iterate on the dashboard UI locally against the live tables.
