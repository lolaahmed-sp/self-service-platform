from __future__ import annotations

import ast
from pathlib import Path

import pytest

from pipeline_platform.config_parser import (
    PipelineConfig,
    SourceConfig,
    DestinationConfig,
    ScheduleConfig,
    TransformConfig,
    QualityChecksConfig,
    NotifyConfig,
)
from pipeline_platform.orchestration.dag_template import render_dag_file
from pipeline_platform.pipeline_generator import generate_dag_file


# ─────────────────────────────────────────────────────────────────────────────
# FIXTURES
# ─────────────────────────────────────────────────────────────────────────────


@pytest.fixture
def sample_config(tmp_path):
    """A minimal valid PipelineConfig for DAG generation tests."""
    return PipelineConfig(
        pipeline_name="test_pipeline",
        owner="analytics",
        source=SourceConfig(type="csv", path="data_sources/customers.csv"),
        destination=DestinationConfig(schema="raw", table="customers"),
        schedule=ScheduleConfig(cron="0 8 * * *"),
        load_mode="append",
        transform=TransformConfig(),
        quality_checks=QualityChecksConfig(),
        notify=NotifyConfig(),
    )


# ─────────────────────────────────────────────────────────────────────────────
# RENDER TESTS - dag template content
# ─────────────────────────────────────────────────────────────────────────────


def test_render_dag_contains_pipeline_name(sample_config):
    """DAG content includes the pipeline name as the dag_id."""
    content = render_dag_file(sample_config)
    assert "test_pipeline" in content


def test_render_dag_contains_cron_schedule(sample_config):
    """DAG content includes the cron schedule."""
    content = render_dag_file(sample_config)
    assert "0 8 * * *" in content


def test_render_dag_contains_config_path(sample_config):
    """DAG content references the correct config file path."""
    content = render_dag_file(sample_config)
    assert "configs/test_pipeline.yaml" in content


def test_render_dag_contains_bash_operator(sample_config):
    """DAG uses BashOperator to run the pipeline."""
    content = render_dag_file(sample_config)
    assert "BashOperator" in content


def test_render_dag_contains_main_py_run_command(sample_config):
    """DAG bash command calls main.py run."""
    content = render_dag_file(sample_config)
    assert "main.py run" in content


def test_render_dag_sets_catchup_false(sample_config):
    """DAG has catchup=False to prevent backfill on first run."""
    content = render_dag_file(sample_config)
    assert "catchup=False" in content


def test_render_dag_is_valid_python(sample_config):
    """Generated DAG content is syntactically valid Python."""
    content = render_dag_file(sample_config)
    try:
        ast.parse(content)
    except SyntaxError as e:
        pytest.fail(f"Generated DAG is not valid Python: {e}")


def test_render_dag_different_pipelines_have_different_dag_ids():
    """Two different pipeline configs produce DAGs with different dag_ids."""
    config_a = PipelineConfig(
        pipeline_name="pipeline_a",
        owner="team",
        source=SourceConfig(type="csv", path="data_sources/a.csv"),
        destination=DestinationConfig(schema="raw", table="a"),
        schedule=ScheduleConfig(cron="0 8 * * *"),
        load_mode="append",
        transform=TransformConfig(),
        quality_checks=QualityChecksConfig(),
        notify=NotifyConfig(),
    )
    config_b = PipelineConfig(
        pipeline_name="pipeline_b",
        owner="team",
        source=SourceConfig(type="csv", path="data_sources/b.csv"),
        destination=DestinationConfig(schema="raw", table="b"),
        schedule=ScheduleConfig(cron="0 12 * * *"),
        load_mode="append",
        transform=TransformConfig(),
        quality_checks=QualityChecksConfig(),
        notify=NotifyConfig(),
    )
    content_a = render_dag_file(config_a)
    content_b = render_dag_file(config_b)

    assert "pipeline_a" in content_a
    assert "pipeline_b" in content_b
    assert "pipeline_b" not in content_a
    assert "pipeline_a" not in content_b


# ─────────────────────────────────────────────────────────────────────────────
# FILE GENERATION TESTS
# ─────────────────────────────────────────────────────────────────────────────


def test_generate_dag_file_creates_file(tmp_path, sample_config, monkeypatch):
    """generate_dag_file writes a .py file to generated_dags/."""
    monkeypatch.chdir(tmp_path)

    output_path = generate_dag_file(sample_config)

    assert Path(output_path).exists()


def test_generate_dag_file_returns_correct_path(tmp_path, sample_config, monkeypatch):
    """generate_dag_file returns the path to the generated file."""
    monkeypatch.chdir(tmp_path)

    output_path = generate_dag_file(sample_config)

    assert "test_pipeline_dag.py" in output_path


def test_generate_dag_file_content_is_valid_python(
    tmp_path, sample_config, monkeypatch
):
    """The written DAG file contains valid Python."""
    monkeypatch.chdir(tmp_path)

    output_path = generate_dag_file(sample_config)
    content = Path(output_path).read_text()

    try:
        ast.parse(content)
    except SyntaxError as e:
        pytest.fail(f"Written DAG file is not valid Python: {e}")


def test_generate_dag_file_overwrites_on_second_call(
    tmp_path, sample_config, monkeypatch
):
    """Calling generate_dag_file twice on the same config overwrites the file."""
    monkeypatch.chdir(tmp_path)

    path1 = generate_dag_file(sample_config)
    path2 = generate_dag_file(sample_config)

    assert path1 == path2
    assert Path(path1).exists()
