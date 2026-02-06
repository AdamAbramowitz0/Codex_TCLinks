"""Load model config from config/model_agents.yaml without external dependencies."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

from tc_market.models import ModelAgentConfig


def _parse_scalar(value: str) -> Any:
    value = value.strip()
    if not value:
        return ""
    if value.startswith(("'", '"')) and value.endswith(("'", '"')) and len(value) >= 2:
        return value[1:-1]

    lowered = value.lower()
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    if lowered == "null":
        return None

    try:
        if "." in value:
            return float(value)
        return int(value)
    except ValueError:
        return value


def _parse_simple_yaml_models(raw: str) -> List[Dict[str, Any]]:
    """Parse a tiny subset of YAML used by our config file.

    Supported format:

    models:
      - id: gpt-5.2
        provider: openai
        model_name: gpt-5.2
        enabled: true
    """

    models: List[Dict[str, Any]] = []
    current: Dict[str, Any] | None = None

    for line in raw.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue

        if stripped.startswith("models:"):
            continue

        if stripped.startswith("- "):
            if current is not None:
                models.append(current)
            current = {}
            remainder = stripped[2:].strip()
            if remainder:
                if ":" not in remainder:
                    raise ValueError(f"Invalid model line: {line}")
                key, value = remainder.split(":", 1)
                current[key.strip()] = _parse_scalar(value)
            continue

        if current is None:
            continue

        if ":" not in stripped:
            raise ValueError(f"Invalid config line: {line}")

        key, value = stripped.split(":", 1)
        current[key.strip()] = _parse_scalar(value)

    if current is not None:
        models.append(current)

    return models


def load_model_configs(path: str | Path) -> List[ModelAgentConfig]:
    config_path = Path(path)
    if not config_path.exists():
        return []

    raw = config_path.read_text(encoding="utf-8").strip()
    if not raw:
        return []

    if raw[0] in "[{":
        payload = json.loads(raw)
        if isinstance(payload, list):
            items = payload
        else:
            items = payload.get("models", [])
    else:
        items = _parse_simple_yaml_models(raw)

    return [ModelAgentConfig.from_dict(item) for item in items]
