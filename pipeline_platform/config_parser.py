from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

import yaml

from pipeline_platform.validator import validate_config_dict


# =========================
# DATA MODELS
# =========================


@dataclass
class SourceConfig:
    type: str
    path: Optional[str] = None

    # API support
    endpoint: Optional[str] = None
    auth_env_var: Optional[str] = None

    # Incremental support
    incremental_key: Optional[str] = None


@dataclass
class DestinationConfig:
    schema: str
    table: str
    write_mode: str = "append"


@dataclass
class ScheduleConfig:
    cron: str
    timezone: Optional[str] = None


@dataclass
class TransformConfig:
    sql: Optional[str] = None
    enabled: bool = False


@dataclass
class PipelineConfig:
    pipeline_name: str
    owner: str
    source: SourceConfig
    destination: DestinationConfig
    schedule: ScheduleConfig
    load_mode: str
    runtime_parameters: dict[str, Any] = field(default_factory=dict)
    transform: TransformConfig = field(default_factory=TransformConfig)


# =========================
# NORMALIZATION HELPERS
# =========================


def _normalize_pipeline(raw: dict) -> dict:
    """Support both flat and nested pipeline structure."""
    if "pipeline" in raw:
        pipeline = raw["pipeline"] or {}
        raw["pipeline_name"] = pipeline.get("name")
        raw["owner"] = pipeline.get("owner")
    return raw


def _normalize_source(raw: dict) -> dict:
    source = raw.get("source", {}) or {}

    source_type = source.get("type")

    if source_type == "api":
        connection = source.get("connection", {}) or {}

        base_url = connection.get("base_url", "")
        endpoint = connection.get("endpoint", "")

        # Build full endpoint safely
        source["endpoint"] = f"{base_url}{endpoint}".rstrip("/")

        source["auth_env_var"] = connection.get("auth_env_var")

        # Incremental extraction mapping
        extraction = source.get("extraction", {}) or {}
        source["incremental_key"] = extraction.get("cursor_field")

    elif source_type == "csv":
        # Support both `path` and `file_path`
        source["path"] = source.get("path") or source.get("file_path")

    raw["source"] = source
    return raw


def _normalize_destination(raw: dict) -> dict:
    dest = raw.get("destination", {}) or {}

    # Align naming differences
    write_mode = dest.get("write_mode") or raw.get("load_mode") or "append"
    dest["write_mode"] = write_mode

    raw["destination"] = dest
    raw["load_mode"] = write_mode

    return raw


def _normalize_transform(raw: dict) -> dict:
    transform = raw.get("transform", {}) or {}

    if transform.get("enabled"):
        transform["sql"] = transform.get("sql_file")

    raw["transform"] = transform
    return raw


# =========================
# MAIN LOADER
# =========================


def load_pipeline_config(path: str) -> PipelineConfig:
    config_path = Path(path)

    with config_path.open("r", encoding="utf-8") as file:
        raw = yaml.safe_load(file) or {}

    # Normalize BEFORE validation
    raw = _normalize_pipeline(raw)
    raw = _normalize_source(raw)
    raw = _normalize_destination(raw)
    raw = _normalize_transform(raw)

    # Validate normalized config
    validate_config_dict(raw)

    return PipelineConfig(
        pipeline_name=raw["pipeline_name"],
        owner=raw["owner"],
        source=SourceConfig(**raw["source"]),
        destination=DestinationConfig(**raw["destination"]),
        schedule=ScheduleConfig(**raw["schedule"]),
        load_mode=raw["load_mode"],
        runtime_parameters=raw.get("runtime_parameters", {}),
        transform=TransformConfig(**raw.get("transform", {})),
    )
