# Student Tasks

This document describes all the extensions students should implement to complete the platform.
Each task builds on the starter codebase and is tagged with its difficulty level.

---

## Tier 1 — Guided (Stubs Already Exist)

These tasks have scaffolding already in place. The field, class, or hook exists — students just need to implement the logic.

---

### Task 1 — Fix Cron Validation

**File:** `pipeline_platform/validator.py`

**Problem:** `schedule.cron` is only checked to be a non-empty string. Invalid expressions like `"every tuesday"` or `"99 99 99 99 99"` pass validation.

**What to do:**
- Install `croniter` and add it to `requirements.txt`
- In `validate_config_dict`, use `croniter` to verify the cron expression is valid
- Raise `ConfigValidationError` if it is not

**Hint:**
```python
from croniter import croniter
croniter.is_valid("0 */6 * * *")  # True
croniter.is_valid("not a cron")   # False
```

---

### Task 2 — Wire Up SQL Transformations

**Files:** `pipeline_platform/pipeline_generator.py`, `pipeline_platform/warehouse/duckdb_client.py`

**Problem:** `TransformConfig.sql` exists in the config model, SQL files exist in `transformations/`, but nothing executes them.

**What to do:**
- Add an `execute_sql_file(path: str)` method to `DuckDBWarehouse` that reads a `.sql` file and runs it
- In `PipelineExecutor.execute()`, after `_load()`, check if `config.transform.sql` is set and call the new method
- Update `configs/sales_orders.yaml` to reference `transformations/clean_sales.sql`

**Hint:** The SQL file path is relative to the project root. Use `pathlib.Path` to read it.

---

### Task 3 — Add API Source Support

**Files:** `pipeline_platform/sources/`, `pipeline_platform/pipeline_generator.py`, `pipeline_platform/validator.py`

**Problem:** `SourceConfig.endpoint` field exists and `_extract()` raises `NotImplementedError` for any non-CSV source type.

**What to do:**
- Create `pipeline_platform/sources/api_ingestor.py` with a class `APIIngestor` that makes a GET request to an endpoint and returns a `pd.DataFrame`
- Register it in `PipelineExecutor._extract()` when `config.source.type == "api"`
- Add `"api"` to `SUPPORTED_SOURCE_TYPES` in `validator.py`
- Add validation that `source.endpoint` is present when `source.type == "api"`

**Hint:** Use the `requests` library. Assume the API returns a JSON array of objects.

---

### Task 4 — Expand the Test Suite

**File:** `tests/`

**Problem:** Only `validator.py` has tests. The executor, registry, run logger, and CSV ingestor have no coverage.

**What to do:**
- Add `tests/test_pipeline_executor.py` — test that a valid config runs end-to-end and logs a run record
- Add `tests/test_csv_ingestor.py` — test that a CSV file is read correctly, and that a missing file raises `FileNotFoundError`
- Add `tests/test_registry.py` — test that `register_pipeline` upserts correctly and `update_last_run_status` updates the record
- Use `tmp_path` (pytest fixture) for any tests that write to DuckDB so they don't pollute the real warehouse

**Hint:**
```python
def test_something(tmp_path):
    db = DuckDBWarehouse(db_path=str(tmp_path / "test.duckdb"))
```

---

## Tier 2 — Design Required (Clear Problem, No Scaffold)

These tasks require students to design the solution — adding new config fields, new tables, or new logic.

---

### Task 5 — Incremental Loading

**Files:** `pipeline_platform/config_parser.py`, `pipeline_platform/validator.py`, `pipeline_platform/pipeline_generator.py`, `configs/`

**Problem:** Every run loads the full source file. For large or frequently updated sources, only new rows should be loaded.

**What to do:**
- Add `incremental_key` (column name) and `incremental_value` (last loaded value) to `SourceConfig`
- In `_extract()`, if `incremental_key` is set, filter the DataFrame to rows where `incremental_key > incremental_value`
- After a successful run, store the new max value of `incremental_key` in a new metadata table `metadata_watermarks`
- On the next run, read the stored watermark and use it as the new `incremental_value`
- Update `configs/sales_orders.yaml` to test incremental loading on `created_at`

---

### Task 6 — Schema Validation

**Files:** `pipeline_platform/warehouse/duckdb_client.py`, `pipeline_platform/metadata/registry.py`, `pipeline_platform/pipeline_generator.py`

**Problem:** If a source CSV changes its column names or types, the pipeline silently loads bad data.

**What to do:**
- On first load, store the column names and types of the loaded table in a new metadata table `metadata_schemas`
- On subsequent runs, before loading, compare the incoming DataFrame schema to the stored schema
- If columns are added, removed, or their types change, raise a `SchemaValidationError` with a clear message describing the difference
- Add a `--force` flag to the CLI `run` command that bypasses the schema check

---

### Task 7 — Data Quality Checks

**Files:** `pipeline_platform/config_parser.py`, `pipeline_platform/pipeline_generator.py`

**Problem:** There is no way to assert anything about the data after it is loaded.

**What to do:**
- Add a `quality_checks` section to the YAML config schema, for example:
  ```yaml
  quality_checks:
    not_null: [order_id, customer_id]
    min_rows: 1
    unique: [order_id]
  ```
- Add a `QualityChecksConfig` dataclass in `config_parser.py`
- In `PipelineExecutor`, after extract, run each check against the DataFrame
- If any check fails, raise a `DataQualityError`, which causes the run to be logged as `FAILED`
- Write tests for at least two check types

---

### Task 8 — Duplicate Pipeline Detection

**Files:** `main.py`, `pipeline_platform/validator.py`

**Problem:** Two YAML configs can define pipelines that write to the same destination table. The second run silently overwrites or appends to data from the first.

**What to do:**
- In the `run-all` command in `main.py`, before executing any pipelines, scan all configs in the directory
- Detect if any two configs share the same `destination.schema` + `destination.table` combination
- If a conflict is found, print a clear error message listing the conflicting pipeline names and exit without running any pipelines
- Write a test that confirms the conflict is caught

---

## Tier 3 — Open-Ended (Architecture Decisions)

Larger tasks with real design choices. Suitable for group projects or final deliverables.

---

### Task 9 — CLI Status Command

**File:** `main.py`

Add a `python main.py status` command that prints a human-readable summary of pipeline health.

The output should include, for each pipeline:
- Pipeline name, owner, schedule
- Last run time and status (`SUCCESS` / `FAILED`)
- Total number of runs and failure count
- Last error message (if any)

All data should be read from `metadata_pipelines` and `metadata_pipeline_runs` in DuckDB.

---

### Task 10 — Failure Notifications

**Files:** `pipeline_platform/config_parser.py`, `pipeline_platform/pipeline_generator.py`

Add a `notify` section to the YAML config:
```yaml
notify:
  on_failure: true
  slack_webhook: "https://hooks.slack.com/services/..."
```

When a pipeline run fails, `PipelineExecutor` should POST a message to the webhook with the pipeline name, error message, and run ID.

Requirements:
- Notification failure must not suppress the original pipeline error
- The webhook URL should be readable from an environment variable as a fallback
- Write a test that mocks the HTTP call and verifies the payload

---

### Task 11 — Parameterized Queries

**Files:** `pipeline_platform/metadata/registry.py`, `pipeline_platform/metadata/run_logger.py`

The current SQL in `registry.py` and `run_logger.py` uses f-string interpolation, which is a SQL injection risk.

Replace all f-string SQL with DuckDB parameterized queries using `?` placeholders:
```python
conn.execute("INSERT INTO ... VALUES (?, ?, ?)", [val1, val2, val3])
```

Requirements:
- All existing tests must still pass after the refactor
- Add a test that proves a pipeline name containing a single quote (e.g. `"o'reilly_pipeline"`) does not break registration

---

### Task 12 — Config Versioning

**Files:** `pipeline_platform/metadata/registry.py`, `pipeline_platform/config_parser.py`

Track the full history of config changes over time.

**What to do:**
- Create a `metadata_config_history` table with columns: `pipeline_name`, `changed_at`, `field_changed`, `old_value`, `new_value`
- In `register_pipeline`, before upserting, compare the incoming config to the stored config
- For each field that has changed, insert a row into `metadata_config_history`
- Add a `python main.py config-history --pipeline <name>` CLI command that prints the change history for a pipeline

---

### Task 13 — Airflow Scheduling with Docker

**Files:** `docker-compose.yml`, `Dockerfile.airflow`, `pipeline_platform/orchestration/dag_template.py`

**Goal:** Replace manual CLI execution with automated Airflow scheduling running inside Docker.

The project already includes a working `docker-compose.yml` and `Dockerfile.airflow`. Your job is to generate real DAGs, get Airflow running, and trigger them.

#### Step 1 — Set up the environment

```bash
cp .env.example .env
```

Edit `.env`:
- Set `AIRFLOW_UID` to your local user ID: `echo $(id -u)`
- Generate a Fernet key and paste it in:
  ```bash
  python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
  ```

#### Step 2 — Build and initialise

```bash
docker compose up airflow-init       # creates the DB schema and admin user
docker compose up --build -d         # start webserver + scheduler
```

Open `http://localhost:8080` and log in with `admin / admin`.

#### Step 3 — Generate DAG files

From your local machine (not inside Docker):
```bash
python main.py generate-dag --config configs/customers.yaml
python main.py generate-dag --config configs/sales_orders.yaml
```

The DAG files are written to `generated_dags/`, which is mounted directly into the Airflow container. They will appear in the Airflow UI within 30 seconds.

#### Step 4 — Trigger a DAG and verify

1. In the Airflow UI, unpause one of your DAGs and trigger it manually
2. Inspect the task logs — you should see the pipeline output
3. Back on your local machine, run:
   ```bash
   python main.py show-table --table metadata_pipeline_runs
   ```
   Confirm the run was logged with status `SUCCESS`

#### Step 5 — Improve the DAG template (student extension)

The current template generates a single-task DAG. Extend `pipeline_platform/orchestration/dag_template.py` to generate a multi-task DAG with explicit stages:

```
ingest >> transform >> quality_check
```

Requirements:
- Each stage should be a separate `BashOperator` task
- If `config.transform.sql` is not set, skip the transform task
- If `config.quality_checks` is not set (Task 7), skip the quality_check task
- Regenerate the DAG files after your changes and verify the new task graph appears in the UI

#### Teardown

```bash
docker compose down          # stop containers, keep volumes
docker compose down -v       # stop containers and delete all volumes (full reset)
```

---

## Submission Checklist

Before submitting, verify:

- [ ] `pytest` passes with no failures
- [ ] `python main.py run-all --config-dir configs` completes without errors
- [ ] `python main.py show-table --table metadata_pipeline_runs` shows successful runs
- [ ] New features have at least one test each
- [ ] `requirements.txt` includes any new dependencies you added
- [ ] (Task 13) DAGs appear in the Airflow UI and trigger successfully
- [ ] (Task 13) Run records exist in `metadata_pipeline_runs` after a DAG run
