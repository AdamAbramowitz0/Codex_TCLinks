"""Typed domain models used by the market engine."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass(frozen=True)
class User:
    id: str
    display_name: str
    email: str
    google_sub: Optional[str]
    account_type: str
    current_chips: int
    created_at: str
    last_daily_credit_date: str


@dataclass(frozen=True)
class Cycle:
    id: str
    cycle_date: str
    status: str
    opened_at: str
    closed_at: Optional[str]


@dataclass(frozen=True)
class CandidateLink:
    id: str
    cycle_id: str
    submitted_by_user_id: str
    original_url: str
    canonical_url: str
    domain: str
    title: str
    created_at: str


@dataclass(frozen=True)
class Pick:
    id: str
    cycle_id: str
    user_id: str
    candidate_id: str
    rank: int
    picked_at: str


@dataclass(frozen=True)
class ModelAgentConfig:
    id: str
    provider: str
    model_name: str
    enabled: bool
    strategy_profile: str
    max_daily_picks: int
    temperature: float
    strategy_plugin: Optional[str] = None

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "ModelAgentConfig":
        missing = [key for key in ["id", "provider", "model_name"] if key not in data]
        if missing:
            raise ValueError(f"Model config missing required keys: {', '.join(missing)}")

        return ModelAgentConfig(
            id=str(data["id"]),
            provider=str(data["provider"]),
            model_name=str(data["model_name"]),
            enabled=bool(data.get("enabled", True)),
            strategy_profile=str(data.get("strategy_profile", "default")),
            max_daily_picks=int(data.get("max_daily_picks", 10)),
            temperature=float(data.get("temperature", 0.2)),
            strategy_plugin=(
                str(data["strategy_plugin"]) if data.get("strategy_plugin") is not None else None
            ),
        )


@dataclass(frozen=True)
class SettlementEntry:
    user_id: str
    correct_count: int
    reward_chips: int
    rank: int


@dataclass(frozen=True)
class ModelPrediction:
    cycle_id: str
    model_user_id: str
    candidate_id: str
    probability: float
    explanation: str
