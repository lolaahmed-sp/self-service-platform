import pytest

from pipeline_platform.validator import (
    ConfigValidationError,
    validate_config_dict,
    reset_registry,
)


# ── Fixture: resets the duplicate-name registry before every test ────────────
@pytest.fixture(autouse=True)
def clear_registry():
    reset_registry()
    yield
    reset_registry()  # clean up after the test too


# ── Shared base config — used by all tests ───────────────────────────────────
# Defined as a function so each test gets its own independent copy.
# Using a module-level dict constant and mutating it would cause tests
# to interfere with each other.
def make_valid_config() -> dict:
    return {
        "pipeline_name": "sales_orders",
        "owner": "analytics",
        "source": {"type": "csv", "path": "data_sources/sales_orders.csv"},
        "destination": {"schema": "raw", "table": "sales_orders"},
        "schedule": {"cron": "@daily"},
        "load_mode": "overwrite",
    }


# Keep VALID_CONFIG for backwards compatibility with existing tests
VALID_CONFIG = {
    "pipeline_name": "sales_orders",
    "owner": "analytics",
    "source": {"type": "csv", "path": "data_sources/sales_orders.csv"},
    "destination": {"schema": "raw", "table": "sales_orders"},
    "schedule": {"cron": "@daily"},
    "load_mode": "overwrite",
}


# ── Existing tests (unchanged) ───────────────────────────────────────────────


def test_validate_accepts_valid_config():
    validate_config_dict(VALID_CONFIG)


def test_validate_rejects_unknown_source_type():
    config = {
        **VALID_CONFIG,
        "source": {"type": "api", "endpoint": "http://example.com/orders"},
    }
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


# ── New cron tests (Task 1 additions — bugs fixed) ───────────────────────────


def test_invalid_cron_expression_fails():
    config = make_valid_config()
    config["schedule"]["cron"] = "every tuesday"
    with pytest.raises(ConfigValidationError):
        validate_config_dict(config)


def test_out_of_range_cron_fails():
    config = make_valid_config()
    config["schedule"]["cron"] = "99 99 99 99 99"
    with pytest.raises(ConfigValidationError):
        validate_config_dict(config)


def test_valid_cron_passes():
    config = make_valid_config()
    config["schedule"]["cron"] = "0 */6 * * *"
    validate_config_dict(config)  # should not raise
