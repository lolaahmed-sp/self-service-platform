import pytest

from pipeline_platform.validator import (
    ConfigValidationError,
    validate_config_dict,
    reset_registry,
)


# =========================
# REGISTRY RESET FIXTURE
# =========================
@pytest.fixture(autouse=True)
def clear_registry():
    reset_registry()
    yield
    reset_registry()


# =========================
# BASE CONFIGS
# =========================

# Flat format — used by existing configs (customers.yaml, sales_orders.yaml)
VALID_CONFIG = {
    "pipeline_name": "sales_orders",
    "owner": "analytics",
    "source": {"type": "csv", "path": "data_sources/sales_orders.csv"},
    "destination": {"schema": "raw", "table": "sales_orders"},
    "schedule": {"cron": "0 8 * * *"},
    "load_mode": "overwrite",
}

# Nested format
BRIEF_CONFIG = {
    "pipeline": {
        "name": "customer_events_pipeline",
        "description": "Ingest customer interaction events",
        "owner": "product_analytics",
        "domain": "customer_platform",
        "version": 1.0,
    },
    "source": {
        "type": "api",
        "connection": {
            "base_url": "https://api.company.internal",
            "endpoint": "/v1/events",
            "method": "GET",
            "auth_type": "bearer_token",
            "auth_env_var": "EVENTS_API_TOKEN",
        },
        "extraction": {
            "mode": "incremental",
            "cursor_field": "event_timestamp",
            "pagination": {
                "type": "offset",
                "page_size": 1000,
            },
        },
        "response_format": "json",
    },
    "destination": {
        "type": "warehouse",
        "warehouse": "postgres",
        "schema": "raw",
        "table": "customer_events",
        "write_mode": "append",
        "primary_key": "event_id",
    },
    "schedule": {
        "cron": "0 */6 * * *",
        "timezone": "UTC",
        "start_date": "2026-01-01",
        "catchup": False,
    },
    "transform": {
        "enabled": True,
        "type": "sql",
        "sql_file": "transforms/customer_events_clean.sql",
        "target_schema": "analytics",
        "target_table": "customer_events",
    },
    "runtime": {
        "retries": 3,
        "retry_delay_minutes": 10,
        "timeout_minutes": 30,
        "alert_email": ["data-platform@company.com"],
    },
    "logging": {
        "log_level": "INFO",
        "capture_row_counts": True,
        "capture_runtime_metrics": True,
        "enable_pipeline_audit": True,
    },
    "data_quality": {
        "enabled": True,
        "checks": [
            {"name": "not_null_event_id", "column": "event_id", "rule": "not_null"},
            {
                "name": "valid_timestamp",
                "column": "event_timestamp",
                "rule": "timestamp_format",
            },
            {
                "name": "positive_user_id",
                "column": "user_id",
                "rule": "greater_than",
                "value": 0,
            },
        ],
    },
}


# =========================
# EXISTING FLAT-FORMAT TESTS
# =========================


def test_validate_accepts_valid_csv_config():
    validate_config_dict(VALID_CONFIG)


def test_validate_accepts_valid_api_config():
    config = {
        **VALID_CONFIG,
        "source": {"type": "api", "endpoint": "http://example.com/orders"},
    }
    validate_config_dict(config)


def test_validate_rejects_unknown_source_type():
    config = {**VALID_CONFIG, "source": {"type": "xml"}}
    with pytest.raises(ConfigValidationError):
        validate_config_dict(config)


def test_validate_rejects_missing_required_field():
    config = {k: v for k, v in VALID_CONFIG.items() if k != "owner"}
    with pytest.raises(ConfigValidationError):
        validate_config_dict(config)


def test_validate_rejects_unsupported_load_mode():
    config = {**VALID_CONFIG, "load_mode": "upsert"}
    with pytest.raises(ConfigValidationError):
        validate_config_dict(config)


def test_validate_rejects_missing_destination_table():
    config = {**VALID_CONFIG, "destination": {"schema": "raw"}}
    with pytest.raises(ConfigValidationError):
        validate_config_dict(config)


def test_validate_rejects_invalid_cron_expression():
    config = {**VALID_CONFIG, "schedule": {"cron": "invalid cron"}}
    with pytest.raises(ConfigValidationError):
        validate_config_dict(config)


def test_validate_rejects_empty_cron():
    config = {**VALID_CONFIG, "schedule": {"cron": ""}}
    with pytest.raises(ConfigValidationError):
        validate_config_dict(config)


def test_validate_rejects_csv_source_without_path():
    config = {**VALID_CONFIG, "source": {"type": "csv"}}
    with pytest.raises(ConfigValidationError):
        validate_config_dict(config)


def test_validate_rejects_api_without_endpoint():
    config = {**VALID_CONFIG, "source": {"type": "api"}}
    with pytest.raises(ConfigValidationError):
        validate_config_dict(config)


def test_validate_rejects_transform_enabled_without_sql():
    config = {**VALID_CONFIG, "transform": {"enabled": True}}
    with pytest.raises(ConfigValidationError):
        validate_config_dict(config)


def test_validate_accepts_transform_with_sql():
    config = {
        **VALID_CONFIG,
        "transform": {"enabled": True, "sql": "transformations/test.sql"},
    }
    validate_config_dict(config)


def test_validate_accepts_incremental_key():
    config = {
        **VALID_CONFIG,
        "source": {
            "type": "csv",
            "path": "data_sources/sales_orders.csv",
            "incremental_key": "created_at",
        },
    }
    validate_config_dict(config)


def test_validate_rejects_invalid_incremental_key():
    config = {
        **VALID_CONFIG,
        "source": {
            "type": "csv",
            "path": "data_sources/sales_orders.csv",
            "incremental_key": 123,
        },
    }
    with pytest.raises(ConfigValidationError):
        validate_config_dict(config)


# =========================
# BRIEF / NESTED FORMAT TESTS
# =========================


def test_validate_accepts_full_brief_config():
    """The complete project brief YAML must pass validation end-to-end."""
    validate_config_dict(BRIEF_CONFIG)


def test_validate_accepts_nested_api_with_connection_block():
    """Nested source.connection format (base_url + endpoint) must be accepted."""
    config = {
        **BRIEF_CONFIG,
        "source": {
            "type": "api",
            "connection": {
                "base_url": "https://api.example.com",
                "endpoint": "/v1/data",
                "auth_type": "bearer_token",
            },
        },
    }
    validate_config_dict(config)


def test_validate_rejects_nested_api_missing_base_url():
    import copy

    config = copy.deepcopy(BRIEF_CONFIG)
    del config["source"]["connection"]["base_url"]
    with pytest.raises(ConfigValidationError, match="base_url"):
        validate_config_dict(config)


def test_validate_rejects_nested_api_missing_endpoint():
    import copy

    config = copy.deepcopy(BRIEF_CONFIG)
    del config["source"]["connection"]["endpoint"]
    with pytest.raises(ConfigValidationError, match="endpoint"):
        validate_config_dict(config)


def test_validate_rejects_invalid_auth_type():
    import copy

    config = copy.deepcopy(BRIEF_CONFIG)
    config["source"]["connection"]["auth_type"] = "magic_token"
    with pytest.raises(ConfigValidationError, match="auth_type"):
        validate_config_dict(config)


def test_validate_rejects_invalid_pagination_type():
    import copy

    config = copy.deepcopy(BRIEF_CONFIG)
    config["source"]["extraction"]["pagination"]["type"] = "infinite_scroll"
    with pytest.raises(ConfigValidationError, match="pagination.type"):
        validate_config_dict(config)


def test_validate_rejects_invalid_page_size():
    import copy

    config = copy.deepcopy(BRIEF_CONFIG)
    config["source"]["extraction"]["pagination"]["page_size"] = -1
    with pytest.raises(ConfigValidationError, match="page_size"):
        validate_config_dict(config)


def test_validate_accepts_schedule_with_start_date():
    config = {
        **VALID_CONFIG,
        "schedule": {
            "cron": "0 8 * * *",
            "start_date": "2026-01-01",
            "timezone": "UTC",
        },
    }
    validate_config_dict(config)


def test_validate_rejects_invalid_start_date():
    config = {
        **VALID_CONFIG,
        "schedule": {"cron": "0 8 * * *", "start_date": "not-a-date"},
    }
    with pytest.raises(ConfigValidationError, match="start_date"):
        validate_config_dict(config)


def test_validate_accepts_full_runtime_block():
    config = {
        **VALID_CONFIG,
        "runtime": {
            "retries": 3,
            "retry_delay_minutes": 10,
            "timeout_minutes": 30,
            "alert_email": ["team@company.com"],
        },
    }
    validate_config_dict(config)


def test_validate_rejects_invalid_alert_email():
    config = {
        **VALID_CONFIG,
        "runtime": {"alert_email": ["not-an-email"]},
    }
    with pytest.raises(ConfigValidationError, match="alert_email"):
        validate_config_dict(config)


def test_validate_rejects_invalid_log_level():
    config = {**VALID_CONFIG, "logging": {"log_level": "VERBOSE"}}
    with pytest.raises(ConfigValidationError, match="log_level"):
        validate_config_dict(config)


def test_validate_accepts_valid_data_quality_block():
    config = {
        **VALID_CONFIG,
        "data_quality": {
            "enabled": True,
            "checks": [
                {"name": "no_nulls", "column": "id", "rule": "not_null"},
                {"name": "unique_id", "column": "id", "rule": "unique"},
                {
                    "name": "pos_amount",
                    "column": "amt",
                    "rule": "greater_than",
                    "value": 0,
                },
            ],
        },
    }
    validate_config_dict(config)


def test_validate_rejects_dq_check_missing_column():
    config = {
        **VALID_CONFIG,
        "data_quality": {
            "enabled": True,
            "checks": [{"name": "check_1", "rule": "not_null"}],
        },
    }
    with pytest.raises(ConfigValidationError, match="column"):
        validate_config_dict(config)


def test_validate_rejects_dq_check_unknown_rule():
    config = {
        **VALID_CONFIG,
        "data_quality": {
            "enabled": True,
            "checks": [{"name": "c", "column": "x", "rule": "magic_check"}],
        },
    }
    with pytest.raises(ConfigValidationError, match="not supported"):
        validate_config_dict(config)


def test_validate_rejects_greater_than_without_value():
    config = {
        **VALID_CONFIG,
        "data_quality": {
            "enabled": True,
            "checks": [{"name": "c", "column": "x", "rule": "greater_than"}],
        },
    }
    with pytest.raises(ConfigValidationError, match="value"):
        validate_config_dict(config)


def test_validate_accepts_transform_with_sql_file():
    """Brief format uses sql_file key — must be accepted alongside sql key."""
    config = {
        **VALID_CONFIG,
        "transform": {
            "enabled": True,
            "sql_file": "transforms/customer_events_clean.sql",
        },
    }
    validate_config_dict(config)
