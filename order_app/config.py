from __future__ import annotations

import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
ORDER_SCHEMA_PATH = BASE_DIR / "schemas" / "order.avsc"
DLQ_SCHEMA_PATH = BASE_DIR / "schemas" / "dlq_order.avsc"


def env_str(name: str, default: str) -> str:
    return os.getenv(name, default)


def env_int(name: str, default: int) -> int:
    return int(os.getenv(name, str(default)))


def env_float(name: str, default: float) -> float:
    return float(os.getenv(name, str(default)))


def env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def env_csv(name: str, default: str = "") -> set[str]:
    value = os.getenv(name, default)
    return {item.strip() for item in value.split(",") if item.strip()}
