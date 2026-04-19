from __future__ import annotations

from pathlib import Path

from pipeline_platform.config_parser import PipelineConfig


DAG_TEMPLATE = """import os
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
"""


def render_dag_file(config: PipelineConfig) -> str:
    return DAG_TEMPLATE.format(
        pipeline_name=config.pipeline_name,
        schedule=config.schedule.cron,
        config_path=f"configs/{config.pipeline_name}.yaml",
    )


def generate_all_dags(config_dir: str = "configs") -> list[str]:
    """
    Generate DAG files for all YAML configs in config_dir.
    Returns a list of paths to the generated files.
    """
    from pipeline_platform.config_parser import load_pipeline_config
    from pipeline_platform.pipeline_generator import generate_dag_file

    config_path = Path(config_dir)
    generated = []
    for yaml_file in sorted(config_path.glob("*.yaml")):
        config = load_pipeline_config(str(yaml_file))
        output_path = generate_dag_file(config)
        generated.append(output_path)
        print(f"[DAG] Generated: {output_path}")
    return generated
