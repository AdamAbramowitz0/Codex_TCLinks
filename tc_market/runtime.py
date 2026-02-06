"""Runtime helpers for local and serverless hosting."""

from __future__ import annotations

import os
from typing import Type

from tc_market.server import APIHandler, create_app

_HANDLER_CLASS: Type[APIHandler] | None = None


def default_db_path() -> str:
    database_url = os.getenv("DATABASE_URL")
    if database_url:
        return database_url
    explicit = os.getenv("DATABASE_PATH")
    if explicit:
        return explicit
    if os.getenv("VERCEL"):
        return "/tmp/tc_market.db"
    return "market.db"


def default_model_config_path() -> str:
    return os.getenv("MODEL_CONFIG_PATH", "config/model_agents.yaml")


def get_handler_class() -> Type[APIHandler]:
    global _HANDLER_CLASS
    if _HANDLER_CLASS is None:
        _HANDLER_CLASS = create_app(
            db_path=default_db_path(),
            config_path=default_model_config_path(),
        )
    return _HANDLER_CLASS
