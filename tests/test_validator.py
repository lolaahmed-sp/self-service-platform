import pytest

from pipeline_platform.validator import ConfigValidationError, validate_config_dict


VALID_CONFIG = {
    "pipeline_name": "sales_orders",
    "owner": "analytics",
    "source": {"type": "csv", "path": "data_sources/sales_orders.csv"},
    "destination": {"schema": "raw", "table": "sales_orders"},
    "schedule": {"cron": "@daily"},
    "load_mode": "overwrite",
}


def test_validate_accepts_valid_config():
    validate_config_dict(VALID_CONFIG)


def test_validate_rejects_unknown_source_type():
    config = {**VALID_CONFIG, "source": {"type": "api", "endpoint": "http://example.com/orders"}}
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


def test_validate_rejects_empty_cron():
    config = {**VALID_CONFIG, "schedule": {"cron": ""}}
    with pytest.raises(ConfigValidationError):
        validate_config_dict(config)


def test_validate_rejects_csv_source_without_path():
    config = {**VALID_CONFIG, "source": {"type": "csv"}}
    with pytest.raises(ConfigValidationError):
        validate_config_dict(config)
