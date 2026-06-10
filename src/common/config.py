from __future__ import annotations

import os
from enum import Enum
from typing import Optional

import yaml
from pydantic import BaseModel

# Path
_BRONZE_BASE = "s3a://datalake/bronze"
_SILVER_BASE = "s3a://datalake/silver"
_CHECKPOINT_BASE = "s3a://datalake/checkpoints"


# Model


class BronzeType(str, Enum):
    CDC = "cdc"
    APPEND = "append"


class CastConfig(BaseModel):
    epoch_days: list[str] = []
    timestamp: list[str] = []


class BronzeConfig(BaseModel):
    name: str
    type: BronzeType
    trigger: str = "10 seconds"

    @property
    def topic(self) -> str:
        if self.type == BronzeType.CDC:
            return f"ecommerce.public.{self.name}"
        return self.name

    @property
    def checkpoint_location(self) -> str:
        return f"{_CHECKPOINT_BASE}/bronze/{self.name}"

    @property
    def output_path(self) -> str:
        return f"{_BRONZE_BASE}/{self.name}"

    @property
    def schema_subject(self) -> str:
        return f"{self.topic}-value"


class SilverConfig(BaseModel):
    name: str
    source_key_cols: list[str]
    merge_key_cols: Optional[list[str]] = None
    cast: CastConfig = CastConfig()
    children: dict[str, SilverConfig] = {}

    @property
    def dedup_key_cols(self) -> list[str]:
        return self.source_key_cols

    @property
    def silver_key_cols(self) -> list[str]:
        return self.merge_key_cols or self.source_key_cols

    @property
    def silver_path(self) -> str:
        return f"{_SILVER_BASE}/{self.name}"

    @property
    def checkpoint_path(self) -> str:
        return f"{_CHECKPOINT_BASE}/silver/{self.name}"


def load_config(path: str = None) -> dict[str, BronzeConfig]:
    if path is None:
        path = os.path.join(os.path.dirname(__file__), "config.yml")

    with open(path) as file:
        raw = yaml.safe_load(file)

    bronze = {
        name: BronzeConfig(name=name, **cfg)
        for name, cfg in raw.get("bronze", {}).items()
    }

    silver = {}
    for name, cfg in raw.get("silver", {}).items():
        children = cfg.pop("children", {})

        children_parsed = {
            child_name: SilverConfig(name=child_name, **child_cfg)
            for child_name, child_cfg in children.items()
        }
        silver[name] = SilverConfig(name=name, children=children_parsed, **cfg)

    return bronze, silver


BRONZE_TABLES, SILVER_TABLES = load_config()
BRONZE_CDC_TABLES = {k: v for k, v in BRONZE_TABLES.items() if v.type == BronzeType.CDC}
BRONZE_APPEND_TABLES = {
    k: v for k, v in BRONZE_TABLES.items() if v.type == BronzeType.APPEND
}
