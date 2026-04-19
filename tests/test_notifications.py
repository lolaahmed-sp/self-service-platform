from __future__ import annotations

import pytest
from unittest.mock import patch, MagicMock

from pipeline_platform.config_parser import (
    PipelineConfig,
    SourceConfig,
    DestinationConfig,
    ScheduleConfig,
    TransformConfig,
    QualityChecksConfig,
    NotifyConfig,
)
from pipeline_platform.notifications.slack import send_failure_notification
from pipeline_platform.pipeline_generator import PipelineExecutor
from pipeline_platform.warehouse.duckdb_client import DuckDBWarehouse


# ─────────────────────────────────────────────────────────────────────────────
# FIXTURES
# ─────────────────────────────────────────────────────────────────────────────


@pytest.fixture
def db(tmp_path):
    return DuckDBWarehouse(db_path=str(tmp_path / "test.duckdb"))


@pytest.fixture
def executor(db):
    return PipelineExecutor(warehouse=db)


# ─────────────────────────────────────────────────────────────────────────────
# UNIT TESTS — send_failure_notification
# ─────────────────────────────────────────────────────────────────────────────


def test_notification_posts_to_webhook():
    """A valid webhook URL receives a POST request."""
    with patch("requests.post") as mock_post:
        send_failure_notification(
            webhook_url="https://hooks.slack.com/test",
            pipeline_name="test_pipe",
            error_message="Something broke",
            run_id="abc-123",
        )
        mock_post.assert_called_once()


def test_notification_payload_contains_pipeline_name():
    """Payload text includes the pipeline name."""
    with patch("requests.post") as mock_post:
        send_failure_notification(
            webhook_url="https://hooks.slack.com/test",
            pipeline_name="sales_orders",
            error_message="File not found",
            run_id="xyz-999",
        )
        payload = mock_post.call_args.kwargs["json"]
        assert "sales_orders" in payload["text"]


def test_notification_payload_contains_run_id():
    """Payload text includes the run ID."""
    with patch("requests.post") as mock_post:
        send_failure_notification(
            webhook_url="https://hooks.slack.com/test",
            pipeline_name="pipe",
            error_message="error",
            run_id="run-id-456",
        )
        payload = mock_post.call_args.kwargs["json"]
        assert "run-id-456" in payload["text"]


def test_notification_payload_contains_error_message():
    """Payload text includes the error message."""
    with patch("requests.post") as mock_post:
        send_failure_notification(
            webhook_url="https://hooks.slack.com/test",
            pipeline_name="pipe",
            error_message="CSV source not found",
            run_id="r1",
        )
        payload = mock_post.call_args.kwargs["json"]
        assert "CSV source not found" in payload["text"]


def test_no_notification_when_webhook_url_is_none():
    """No HTTP call is made when webhook_url is None and env var not set."""
    with patch("requests.post") as mock_post:
        with patch.dict("os.environ", {}, clear=True):
            send_failure_notification(
                webhook_url=None,
                pipeline_name="pipe",
                error_message="error",
                run_id="r1",
            )
        mock_post.assert_not_called()


def test_notification_uses_env_var_when_url_not_provided():
    """Falls back to SLACK_WEBHOOK_URL env var when webhook_url is None."""
    with patch("requests.post") as mock_post:
        with patch.dict(
            "os.environ", {"SLACK_WEBHOOK_URL": "https://hooks.slack.com/env"}
        ):
            send_failure_notification(
                webhook_url=None,
                pipeline_name="pipe",
                error_message="error",
                run_id="r1",
            )
        mock_post.assert_called_once()


def test_notification_failure_does_not_raise():
    """A network error during notification must not propagate."""
    with patch("requests.post", side_effect=Exception("network error")):
        # Should complete without raising
        send_failure_notification(
            webhook_url="https://hooks.slack.com/test",
            pipeline_name="pipe",
            error_message="error",
            run_id="r1",
        )


# ─────────────────────────────────────────────────────────────────────────────
# INTEGRATION TESTS — notification triggered by pipeline failure
# ─────────────────────────────────────────────────────────────────────────────


def test_notification_sent_on_pipeline_failure(tmp_path, db, executor):
    """When on_failure=True and pipeline fails, webhook is called."""
    config = PipelineConfig(
        pipeline_name="notify_fail",
        owner="test",
        source=SourceConfig(
            type="csv",
            path=str(tmp_path / "nonexistent.csv"),
        ),
        destination=DestinationConfig(schema="raw", table="notify_fail"),
        schedule=ScheduleConfig(cron="0 8 * * *"),
        load_mode="append",
        transform=TransformConfig(),
        quality_checks=QualityChecksConfig(),
        notify=NotifyConfig(
            on_failure=True,
            slack_webhook="https://hooks.slack.com/test",
        ),
    )

    with patch("requests.post") as mock_post:
        result = executor.execute(config)

    assert result["status"] == "failed"
    mock_post.assert_called_once()


def test_notification_not_sent_on_pipeline_success(tmp_path, db, executor):
    """When pipeline succeeds, webhook is NOT called."""
    csv_file = tmp_path / "data.csv"
    csv_file.write_text("id,name\n1,Ada\n2,Tomi\n")

    config = PipelineConfig(
        pipeline_name="notify_success",
        owner="test",
        source=SourceConfig(type="csv", path=str(csv_file)),
        destination=DestinationConfig(schema="raw", table="notify_success"),
        schedule=ScheduleConfig(cron="0 8 * * *"),
        load_mode="append",
        transform=TransformConfig(),
        quality_checks=QualityChecksConfig(),
        notify=NotifyConfig(
            on_failure=True,
            slack_webhook="https://hooks.slack.com/test",
        ),
    )

    with patch("requests.post") as mock_post:
        result = executor.execute(config)

    assert result["status"] == "success"
    mock_post.assert_not_called()


def test_notification_not_sent_when_on_failure_false(tmp_path, db, executor):
    """When on_failure=False, webhook is NOT called even on failure."""
    config = PipelineConfig(
        pipeline_name="notify_disabled",
        owner="test",
        source=SourceConfig(
            type="csv",
            path=str(tmp_path / "nonexistent.csv"),
        ),
        destination=DestinationConfig(schema="raw", table="notify_disabled"),
        schedule=ScheduleConfig(cron="0 8 * * *"),
        load_mode="append",
        transform=TransformConfig(),
        quality_checks=QualityChecksConfig(),
        notify=NotifyConfig(on_failure=False),
    )

    with patch("requests.post") as mock_post:
        result = executor.execute(config)

    assert result["status"] == "failed"
    mock_post.assert_not_called()
