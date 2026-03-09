# Self-Service Data Pipeline Platform

A metadata-driven pipeline platform for students to extend.

## What this starter does
- Defines pipelines with YAML config files
- Validates config on load
- Ingests CSV sources into a local DuckDB warehouse
- Supports `append` and `overwrite` load modes
- Tracks pipeline metadata and per-run history
- Generates Airflow DAG stubs from config

## Setup

```bash
python -m venv .venv
source .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

## Run a pipeline

```bash
python main.py run --config configs/customers.yaml
python main.py run --config configs/sales_orders.yaml
```

Run all pipelines in a directory:
```bash
python main.py run-all --config-dir configs
```

## Inspect the warehouse

```bash
python main.py show-table --table raw_customers
python main.py show-table --table raw_sales_orders
python main.py show-table --table metadata_pipelines
python main.py show-table --table metadata_pipeline_runs
```

## Generate a DAG stub

```bash
python main.py generate-dag --config configs/sales_orders.yaml
```

## Run tests

```bash
pytest
```

## Makefile shortcuts

```bash
make install          # create .venv and install dependencies

make run-customers    # run the customers pipeline
make run-sales        # run the sales_orders pipeline
make run-all          # run all pipelines

make dag              # generate DAG stub for sales_orders
make dags             # generate DAG stubs for all configs

make show-customers   # show raw_customers table
make show-sales       # show raw_sales_orders table
make show-registry    # show pipeline registry
make show-runs        # show run history

make test             # run tests
make clean            # delete warehouse.duckdb (fresh start)
```

## Airflow (Docker)

```bash
cp .env.example .env  # fill in AIRFLOW_UID and FERNET_KEY (see Task 13)

make airflow-init     # initialise the Airflow DB and create admin user
make airflow-up       # start webserver + scheduler (http://localhost:8080)
make airflow-down     # stop containers (keep volumes)
make airflow-reset    # stop containers and delete all volumes (full reset)
```

## Project structure

```
self_service_pipeline_platform/
├── configs/                  # YAML pipeline definitions
├── data_sources/             # Sample CSV source files
├── pipeline_platform/
│   ├── config_parser.py      # YAML -> dataclass models
│   ├── validator.py          # Config validation rules
│   ├── pipeline_generator.py # Execution engine + DAG generation
│   ├── sources/
│   │   └── csv_ingestor.py   # CSV -> DataFrame
│   ├── warehouse/
│   │   └── duckdb_client.py  # DuckDB persistence
│   ├── metadata/
│   │   ├── registry.py       # Pipeline registration
│   │   └── run_logger.py     # Run history logging
│   └── orchestration/
│       └── dag_template.py   # Airflow DAG stub template
├── transformations/          # SQL transformation files (not yet wired up)
├── tests/
│   └── test_validator.py
├── main.py
├── requirements.txt
└── Makefile
```

## What students should add

This starter is intentionally incomplete. Suggested extensions:

### Tier 1 — Stubs already exist
1. **Wire up SQL transformations** — `TransformConfig.sql` field and SQL files in `transformations/` exist; connect them to the executor
2. **Add API source support** — `SourceConfig.endpoint` field exists; add an `APIIngestor` and register it in `_extract()`
3. **Fix cron validation** — `validator.py` only checks for a non-empty string; use a real cron parser (e.g. `croniter`)
4. **Expand the test suite** — add fixtures and coverage for the executor, registry, and run logger

### Tier 2 — Design required
5. **Incremental loading** — add `incremental_key` to config; filter source on extract, store watermark in metadata
6. **Schema validation** — detect column drift between pipeline runs
7. **Data quality checks** — add a `quality_checks` section to YAML config (e.g. `not_null`, `min_rows`)
8. **Duplicate pipeline detection** — warn when two configs target the same destination table

### Tier 3 — Open-ended
9. **CLI status command** — `python main.py status` showing a summary of recent runs
10. **Failure notifications** — add Slack/email hooks on pipeline failure
11. **Parameterized queries** — replace f-string SQL in registry/run_logger with parameterized queries
12. **Config versioning** — track config changes in a `metadata_config_history` table
13. **Airflow scheduling with Docker** — generate real DAGs and run them via the included Docker Compose setup

## Notes
- The warehouse is DuckDB — no database server needed.
- The DAG generator writes Python files to `generated_dags/` as a scaffold for Airflow.
- Run `make clean` to reset the warehouse and start fresh.
