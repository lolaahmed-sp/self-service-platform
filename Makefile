.PHONY: install run-customers run-sales run-all dag show-customers show-sales show-registry show-runs test clean airflow-init airflow-up airflow-down airflow-reset dags

install:
	python -m venv .venv && . .venv/bin/activate && pip install -r requirements.txt

run-customers:
	python main.py run --config configs/customers.yaml

run-sales:
	python main.py run --config configs/sales_orders.yaml

run-all:
	python main.py run-all --config-dir configs

dag:
	python main.py generate-dag --config configs/sales_orders.yaml

show-customers:
	python main.py show-table --table raw_customers

show-sales:
	python main.py show-table --table raw_sales_orders

show-registry:
	python main.py show-table --table metadata_pipelines

show-runs:
	python main.py show-table --table metadata_pipeline_runs

test:
	pytest

clean:
	rm -f warehouse.duckdb

# ── Docker / Airflow ──────────────────────────────────────────────────────────

airflow-init:
	docker compose up airflow-init

airflow-up:
	docker compose up --build -d airflow-webserver airflow-scheduler

airflow-down:
	docker compose down

airflow-reset:
	docker compose down -v

dags:
	python main.py generate-dag --config configs/customers.yaml
	python main.py generate-dag --config configs/sales_orders.yaml
