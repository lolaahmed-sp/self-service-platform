from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from pipeline_platform.validator import validate_config_dict


@dataclass
class SourceConfig:
    type: str
    path: str | None = None
    endpoint: str | None = None
    incremental_key: str | None = None


@dataclass
class DestinationConfig:
    schema: str
    table: str


@dataclass
class ScheduleConfig:
    cron: str


@dataclass
class TransformConfig:
    sql: str | None = None


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


def load_pipeline_config(path: str) -> PipelineConfig:
    config_path = Path(path)
    with config_path.open("r", encoding="utf-8") as file:
        raw = yaml.safe_load(file)

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
