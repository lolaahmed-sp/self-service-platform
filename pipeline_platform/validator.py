from __future__ import annotations

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


class ConfigValidationError(ValueError):
    """Raised when a pipeline configuration is invalid."""



def validate_config_dict(config: dict) -> None:
    missing_fields = REQUIRED_TOP_LEVEL_FIELDS - set(config.keys())
    if missing_fields:
        raise ConfigValidationError(
            f"Missing required top-level fields: {sorted(missing_fields)}"
        )

    if config["source"].get("type") not in SUPPORTED_SOURCE_TYPES:
        raise ConfigValidationError(
            f"Unsupported source type: {config['source'].get('type')}. "
            f"Supported: {sorted(SUPPORTED_SOURCE_TYPES)}"
        )

    if config["load_mode"] not in SUPPORTED_LOAD_MODES:
        raise ConfigValidationError(
            f"Unsupported load_mode: {config['load_mode']}. "
            f"Supported: {sorted(SUPPORTED_LOAD_MODES)}"
        )

    if not config["destination"].get("table"):
        raise ConfigValidationError("destination.table is required")

    if not config["destination"].get("schema"):
        raise ConfigValidationError("destination.schema is required")

    cron_value = config["schedule"].get("cron")
    if not cron_value or not isinstance(cron_value, str):
        raise ConfigValidationError("schedule.cron must be a non-empty string")

    if config["source"].get("type") == "csv" and not config["source"].get("path"):
        raise ConfigValidationError("source.path is required when source.type = csv")
