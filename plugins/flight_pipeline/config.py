"""Typed access to config/pipeline.yml.

Paths are resolved from this file's own location rather than a hardcoded
`/opt/airflow`. The repository layout and the container layout are the same
shape — `plugins/`, `include/` and `config/` all sit beside each other — so
`parents[2]` lands on the project root in both, and the same code works in a
container, in CI, and in a bare checkout with no environment variables set.

Environment variables still override the three values that `.env` has always
exposed, so existing deployment habits keep working.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

import yaml

# plugins/flight_pipeline/config.py -> plugins/flight_pipeline -> plugins -> ROOT
PROJECT_ROOT = Path(__file__).resolve().parents[2]
INCLUDE_DIR = PROJECT_ROOT / "include"
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "config" / "pipeline.yml"


@dataclass(frozen=True)
class Connections:
    mysql: str
    postgres: str


@dataclass(frozen=True)
class Paths:
    source_csv: Path
    reference_airports: Path
    reference_allowed_values: Path
    corrupted_fixture: Path
    sql: Path


@dataclass(frozen=True)
class Validation:
    reject_rate_threshold: float
    fare_tolerance_bdt: float
    datetime_format: str
    datetime_regex: str


@dataclass(frozen=True)
class BusinessRules:
    fare_markup_factor: float
    regular_season_label: str


@dataclass(frozen=True)
class Config:
    connections: Connections
    paths: Paths
    validation: Validation
    business_rules: BusinessRules
    ingest_batch_size: int


def _resolve(rel: str) -> Path:
    """Interpret a config path as relative to include/, unless absolute."""
    p = Path(rel)
    return p if p.is_absolute() else INCLUDE_DIR / p


@lru_cache(maxsize=1)
def get_config(config_path: str | None = None) -> Config:
    path = Path(config_path or os.getenv("PIPELINE_CONFIG") or DEFAULT_CONFIG_PATH)
    if not path.is_file():
        raise FileNotFoundError(f"pipeline config not found: {path}")

    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    conns = raw.get("connections", {})
    paths = raw.get("paths", {})
    val = raw.get("validation", {})
    rules = raw.get("business_rules", {})

    # Environment wins over the file, for the values .env has always carried.
    source_csv = os.getenv("SOURCE_CSV_PATH") or paths["source_csv"]
    threshold = float(os.getenv("REJECT_RATE_THRESHOLD", val["reject_rate_threshold"]))
    tolerance = float(os.getenv("FARE_TOLERANCE_BDT", val["fare_tolerance_bdt"]))

    if not 0 <= threshold <= 1:
        raise ValueError(f"reject_rate_threshold must be in [0, 1], got {threshold}")
    if tolerance < 0:
        raise ValueError(f"fare_tolerance_bdt must be >= 0, got {tolerance}")
    if float(rules["fare_markup_factor"]) <= 0:
        raise ValueError("fare_markup_factor must be positive")

    return Config(
        connections=Connections(mysql=conns["mysql"], postgres=conns["postgres"]),
        paths=Paths(
            source_csv=_resolve(source_csv),
            reference_airports=_resolve(paths["reference_airports"]),
            reference_allowed_values=_resolve(paths["reference_allowed_values"]),
            corrupted_fixture=_resolve(paths["corrupted_fixture"]),
            sql=_resolve(paths["sql"]),
        ),
        validation=Validation(
            reject_rate_threshold=threshold,
            fare_tolerance_bdt=tolerance,
            datetime_format=val["datetime_format"],
            datetime_regex=val["datetime_regex"],
        ),
        business_rules=BusinessRules(
            fare_markup_factor=float(rules["fare_markup_factor"]),
            regular_season_label=rules["regular_season_label"],
        ),
        ingest_batch_size=int(raw.get("ingest", {})["batch_size"]),
    )
