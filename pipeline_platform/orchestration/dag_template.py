from __future__ import annotations

from pipeline_platform.config_parser import PipelineConfig


DAG_TEMPLATE = '''import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# When running locally: project root is the working directory.
# When running inside Docker: project is mounted at PIPELINE_PROJECT_DIR.
_PROJECT_DIR = os.getenv("PIPELINE_PROJECT_DIR", ".")

with DAG(
    dag_id="{pipeline_name}",
    start_date=datetime(2026, 1, 1),
    schedule="{schedule}",
    catchup=False,
    tags=["starter-platform", "generated"],
) as dag:
    run_pipeline = BashOperator(
        task_id="run_pipeline",
        bash_command="cd " + _PROJECT_DIR + " && python main.py run --config {config_path}",
    )
'''


def render_dag_file(config: PipelineConfig) -> str:
    return DAG_TEMPLATE.format(
        pipeline_name=config.pipeline_name,
        schedule=config.schedule.cron,
        config_path=f"configs/{config.pipeline_name}.yaml",
    )
