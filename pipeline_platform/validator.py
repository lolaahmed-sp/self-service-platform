from __future__ import annotations
from croniter import croniter

SUPPORTED_SOURCE_TYPES = {"csv"}
SUPPORTED_LOAD_MODES = {"append", "overwrite"}
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


def validate_config_dict(config: dict) -> None:
    """
    Validates a pipeline configuration dictionary against all platform rules.

    Checks performed:
    - Required top-level fields present
    - Source type is supported
    - Load mode is supported
    - Destination schema and table are present
    - Cron expression is valid (via croniter)
    - No duplicate pipeline names
    - CSV source has a path defined
    - Transformation block (if present) has sql_file when enabled
    - Runtime block (if present) has valid retries type

    Raises:
        ConfigValidationError: if any validation rule fails.
    """

    # ── 1. Required top-level fields ────────────────────────────────────────
    missing_fields = REQUIRED_TOP_LEVEL_FIELDS - set(config.keys())
    if missing_fields:
        raise ConfigValidationError(
            f"Missing required top-level fields: {sorted(missing_fields)}"
        )

    # ── 2. Source type check ─────────────────────────────────────────────────
    if config["source"].get("type") not in SUPPORTED_SOURCE_TYPES:
        raise ConfigValidationError(
            f"Unsupported source type: '{config['source'].get('type')}'. "
            f"Supported: {sorted(SUPPORTED_SOURCE_TYPES)}"
        )

    # ── 3. Load mode check ───────────────────────────────────────────────────
    if config["load_mode"] not in SUPPORTED_LOAD_MODES:
        raise ConfigValidationError(
            f"Unsupported load_mode: '{config['load_mode']}'. "
            f"Supported: {sorted(SUPPORTED_LOAD_MODES)}"
        )

    # ── 4. Destination completeness ──────────────────────────────────────────
    if not config["destination"].get("table"):
        raise ConfigValidationError("destination.table is required")

    if not config["destination"].get("schema"):
        raise ConfigValidationError("destination.schema is required")

    # ── 5. Cron expression validation ────────────────────────────────────────
    cron_expr = config["schedule"].get("cron", "")
    if not cron_expr:
        raise ConfigValidationError("schedule.cron is required")
    if not croniter.is_valid(cron_expr):
        raise ConfigValidationError(
            f"Invalid cron expression: '{cron_expr}'. "
            "Expected 5 fields: minute hour day month weekday. "
            "Example: '0 */6 * * *'"
        )

    # ── 6. Duplicate pipeline name detection ─────────────────────────────────
    pipeline_name = config.get("pipeline_name", "")
    if pipeline_name in _REGISTERED_PIPELINE_NAMES:
        raise ConfigValidationError(
            f"Duplicate pipeline name detected: '{pipeline_name}'. "
            "Each pipeline must have a unique name."
        )
    _REGISTERED_PIPELINE_NAMES.add(pipeline_name)

    # ── 7. CSV source path check ─────────────────────────────────────────────
    if config["source"].get("type") == "csv" and not config["source"].get("path"):
        raise ConfigValidationError("source.path is required when source.type = csv")

    # ── 8. Transformation block (optional — only validated if present) ────────
    if "transformation" in config:
        transform = config["transformation"]
        if transform.get("enabled") and not transform.get("sql_file"):
            raise ConfigValidationError(
                "transformation.sql_file is required when transformation.enabled = true"
            )

    # ── 9. Runtime block (optional — only validated if present) ──────────────
    if "runtime" in config:
        runtime = config["runtime"]
        if "retries" in runtime and not isinstance(runtime["retries"], int):
            raise ConfigValidationError("runtime.retries must be an integer")
        if "retry_delay_minutes" in runtime and not isinstance(
            runtime["retry_delay_minutes"], (int, float)
        ):
            raise ConfigValidationError("runtime.retry_delay_minutes must be a number")


def reset_registry() -> None:
    """
    Clears the in-memory duplicate name registry.
    Call this between test runs to avoid false duplicate errors.
    """
    _REGISTERED_PIPELINE_NAMES.clear()
