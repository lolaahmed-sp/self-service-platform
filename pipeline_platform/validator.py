from __future__ import annotations

from croniter import croniter


SUPPORTED_SOURCE_TYPES = {"csv", "api"}
SUPPORTED_LOAD_MODES = {"append", "overwrite"}
SUPPORTED_LOG_LEVELS = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
SUPPORTED_DQ_RULES = {
    "not_null",
    "unique",
    "timestamp_format",
    "greater_than",
    "min_rows",
}
SUPPORTED_AUTH_TYPES = {"bearer_token", "api_key", "basic", "none"}
SUPPORTED_PAGINATION_TYPES = {"offset", "cursor", "page"}
SUPPORTED_EXTRACTION_MODES = {"full", "incremental"}

REQUIRED_TOP_LEVEL_FIELDS = {
    "pipeline_name",
    "owner",
    "source",
    "destination",
    "schedule",
    "load_mode",
}

# Tracks pipeline names across calls to detect duplicates
_REGISTERED_PIPELINE_NAMES: set[str] = set()


class ConfigValidationError(ValueError):
    """Raised when a pipeline configuration is invalid."""


# ─────────────────────────────────────────────────────────────────────────────
# PUBLIC ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────


def validate_config_dict(config: dict) -> None:
    """
    Validates a pipeline configuration dictionary.

    Accepts two formats:
      - Flat format  (existing configs): top-level pipeline_name, owner, load_mode
      - Nested format (brief YAML):      pipeline.name, pipeline.owner,
                                         destination.write_mode

    Raises:
        ConfigValidationError: if any validation rule fails.
    """
    normalised = _normalise(config)

    _validate_required_fields(normalised)
    _validate_source(normalised["source"])
    _validate_destination(normalised["destination"])
    _validate_schedule(normalised["schedule"])
    _validate_load_mode(normalised["load_mode"])
    _validate_duplicate_name(normalised["pipeline_name"])

    if normalised.get("transform"):
        _validate_transform(normalised["transform"])

    if normalised.get("runtime"):
        _validate_runtime(normalised["runtime"])

    if normalised.get("logging"):
        _validate_logging(normalised["logging"])

    if normalised.get("data_quality"):
        _validate_data_quality(normalised["data_quality"])

    # Register name only after all checks pass
    _REGISTERED_PIPELINE_NAMES.add(normalised["pipeline_name"])


def reset_registry() -> None:
    """
    Clears the in-memory duplicate name registry.
    Call this between test runs to avoid false duplicate errors.
    """
    _REGISTERED_PIPELINE_NAMES.clear()


# ─────────────────────────────────────────────────────────────────────────────
# NORMALISATION
# ─────────────────────────────────────────────────────────────────────────────


def _normalise(config: dict) -> dict:
    """
    Convert either config format into a consistent internal dict.

    Flat format (existing configs):
        pipeline_name: "sales_orders"
        owner: "analytics"
        load_mode: "overwrite"

    Nested format (project brief):
        pipeline:
          name: "customer_events_pipeline"
          owner: "product_analytics"
        destination:
          write_mode: "append"
    """
    if "pipeline_name" in config:
        return config

    pipeline_block = config.get("pipeline", {})
    if not isinstance(pipeline_block, dict):
        raise ConfigValidationError("'pipeline' must be a dictionary")

    pipeline_name = pipeline_block.get("name", "")
    owner = pipeline_block.get("owner", "")

    dest = config.get("destination", {})
    load_mode = dest.get("write_mode", "") if isinstance(dest, dict) else ""

    return {
        **config,
        "pipeline_name": pipeline_name,
        "owner": owner,
        "load_mode": load_mode,
    }


# ─────────────────────────────────────────────────────────────────────────────
# SECTION VALIDATORS
# ─────────────────────────────────────────────────────────────────────────────


def _validate_required_fields(config: dict) -> None:
    missing = REQUIRED_TOP_LEVEL_FIELDS - set(config.keys())
    if missing:
        raise ConfigValidationError(f"Missing required fields: {sorted(missing)}")

    if not config["pipeline_name"]:
        raise ConfigValidationError(
            "pipeline_name (or pipeline.name) must not be empty"
        )

    if not config["owner"]:
        raise ConfigValidationError("owner (or pipeline.owner) must not be empty")

    for field in ("source", "destination", "schedule"):
        if not isinstance(config[field], dict):
            raise ConfigValidationError(f"'{field}' must be a dictionary")


def _validate_source(source: dict) -> None:
    source_type = source.get("type")

    if source_type not in SUPPORTED_SOURCE_TYPES:
        raise ConfigValidationError(
            f"Unsupported source type: '{source_type}'. "
            f"Supported: {sorted(SUPPORTED_SOURCE_TYPES)}"
        )

    if source_type == "csv":
        if not source.get("path"):
            raise ConfigValidationError(
                "source.path is required when source.type = 'csv'"
            )

    if source_type == "api":
        _validate_api_source(source)

    # Task 5 — incremental_key must be a string if provided
    incremental_key = source.get("incremental_key")
    if incremental_key is not None and not isinstance(incremental_key, str):
        raise ConfigValidationError(
            "source.incremental_key must be a string column name"
        )


def _validate_api_source(source: dict) -> None:
    """
    Accepts two API source shapes:

    Flat (Task 3 style):
        source:
          type: api
          endpoint: "http://..."

    Nested (brief style):
        source:
          type: api
          connection:
            base_url: "https://..."
            endpoint: "/v1/events"
    """
    connection = source.get("connection")

    if connection:
        if not isinstance(connection, dict):
            raise ConfigValidationError("source.connection must be a dictionary")
        if not connection.get("base_url"):
            raise ConfigValidationError(
                "source.connection.base_url is required for API sources"
            )
        if not connection.get("endpoint"):
            raise ConfigValidationError(
                "source.connection.endpoint is required for API sources"
            )
        auth_type = connection.get("auth_type")
        if auth_type and auth_type not in SUPPORTED_AUTH_TYPES:
            raise ConfigValidationError(
                f"Unsupported auth_type: '{auth_type}'. "
                f"Supported: {sorted(SUPPORTED_AUTH_TYPES)}"
            )
        extraction = source.get("extraction", {})
        if extraction:
            _validate_extraction(extraction)
    else:
        if not source.get("endpoint"):
            raise ConfigValidationError(
                "source.endpoint is required when source.type = 'api'"
            )


def _validate_extraction(extraction: dict) -> None:
    mode = extraction.get("mode")
    if mode and mode not in SUPPORTED_EXTRACTION_MODES:
        raise ConfigValidationError(
            f"Unsupported extraction.mode: '{mode}'. "
            f"Supported: {sorted(SUPPORTED_EXTRACTION_MODES)}"
        )

    pagination = extraction.get("pagination", {})
    if pagination:
        if not isinstance(pagination, dict):
            raise ConfigValidationError(
                "source.extraction.pagination must be a dictionary"
            )
        pag_type = pagination.get("type")
        if pag_type and pag_type not in SUPPORTED_PAGINATION_TYPES:
            raise ConfigValidationError(
                f"Unsupported pagination.type: '{pag_type}'. "
                f"Supported: {sorted(SUPPORTED_PAGINATION_TYPES)}"
            )
        page_size = pagination.get("page_size")
        if page_size is not None:
            if not isinstance(page_size, int) or page_size < 1:
                raise ConfigValidationError(
                    "source.extraction.pagination.page_size must be a positive integer"
                )


def _validate_destination(destination: dict) -> None:
    if not destination.get("table"):
        raise ConfigValidationError("destination.table is required")
    if not destination.get("schema"):
        raise ConfigValidationError("destination.schema is required")


def _validate_schedule(schedule: dict) -> None:
    cron_value = schedule.get("cron")

    if not cron_value or not isinstance(cron_value, str):
        raise ConfigValidationError("schedule.cron must be a non-empty string")

    if not croniter.is_valid(cron_value):
        raise ConfigValidationError(
            f"Invalid cron expression: '{cron_value}'. "
            "Expected format: 'minute hour day month weekday' "
            "(e.g. '0 */6 * * *')"
        )

    start_date = schedule.get("start_date")
    if start_date is not None:
        try:
            from datetime import date

            if isinstance(start_date, str):
                date.fromisoformat(start_date)
        except ValueError:
            raise ConfigValidationError(
                f"schedule.start_date must be a valid ISO date "
                f"(YYYY-MM-DD), got: '{start_date}'"
            )


def _validate_load_mode(load_mode: str) -> None:
    if load_mode not in SUPPORTED_LOAD_MODES:
        raise ConfigValidationError(
            f"Unsupported load_mode: '{load_mode}'. "
            f"Supported: {sorted(SUPPORTED_LOAD_MODES)}"
        )


def _validate_duplicate_name(pipeline_name: str) -> None:
    if pipeline_name in _REGISTERED_PIPELINE_NAMES:
        raise ConfigValidationError(
            f"Duplicate pipeline name detected: '{pipeline_name}'. "
            "Each pipeline must have a unique name."
        )


def _validate_transform(transform: dict) -> None:
    if not isinstance(transform, dict):
        raise ConfigValidationError("'transform' must be a dictionary")

    if transform.get("enabled"):
        has_sql = transform.get("sql") or transform.get("sql_file")
        if not has_sql:
            raise ConfigValidationError(
                "transform.sql (or transform.sql_file) is required "
                "when transform.enabled = true"
            )


def _validate_runtime(runtime: dict) -> None:
    if not isinstance(runtime, dict):
        raise ConfigValidationError("'runtime' must be a dictionary")

    retries = runtime.get("retries")
    if retries is not None and not isinstance(retries, int):
        raise ConfigValidationError("runtime.retries must be an integer")

    retry_delay = runtime.get("retry_delay_minutes")
    if retry_delay is not None and not isinstance(retry_delay, (int, float)):
        raise ConfigValidationError("runtime.retry_delay_minutes must be a number")

    timeout = runtime.get("timeout_minutes")
    if timeout is not None and not isinstance(timeout, (int, float)):
        raise ConfigValidationError("runtime.timeout_minutes must be a number")

    alert_email = runtime.get("alert_email")
    if alert_email is not None:
        if not isinstance(alert_email, list):
            raise ConfigValidationError("runtime.alert_email must be a list")
        for addr in alert_email:
            if not isinstance(addr, str) or "@" not in addr:
                raise ConfigValidationError(
                    f"runtime.alert_email contains an invalid address: '{addr}'"
                )


def _validate_logging(logging_cfg: dict) -> None:
    if not isinstance(logging_cfg, dict):
        raise ConfigValidationError("'logging' must be a dictionary")

    log_level = logging_cfg.get("log_level")
    if log_level and log_level not in SUPPORTED_LOG_LEVELS:
        raise ConfigValidationError(
            f"logging.log_level must be one of "
            f"{sorted(SUPPORTED_LOG_LEVELS)}, got: '{log_level}'"
        )

    for bool_field in (
        "capture_row_counts",
        "capture_runtime_metrics",
        "enable_pipeline_audit",
    ):
        val = logging_cfg.get(bool_field)
        if val is not None and not isinstance(val, bool):
            raise ConfigValidationError(f"logging.{bool_field} must be true or false")


def _validate_data_quality(dq: dict) -> None:
    if not isinstance(dq, dict):
        raise ConfigValidationError("'data_quality' must be a dictionary")

    checks = dq.get("checks", [])
    if not isinstance(checks, list):
        raise ConfigValidationError("data_quality.checks must be a list")

    for i, check in enumerate(checks):
        label = f"data_quality.checks[{i}]"

        if not isinstance(check, dict):
            raise ConfigValidationError(f"{label} must be a dictionary")

        if not check.get("name"):
            raise ConfigValidationError(f"{label}.name is required")

        if not check.get("column"):
            raise ConfigValidationError(f"{label}.column is required")

        rule = check.get("rule")
        if not rule:
            raise ConfigValidationError(f"{label}.rule is required")

        if rule not in SUPPORTED_DQ_RULES:
            raise ConfigValidationError(
                f"{label}.rule '{rule}' is not supported. "
                f"Supported: {sorted(SUPPORTED_DQ_RULES)}"
            )

        if rule == "greater_than" and "value" not in check:
            raise ConfigValidationError(
                f"{label}: rule 'greater_than' requires a 'value' field"
            )
