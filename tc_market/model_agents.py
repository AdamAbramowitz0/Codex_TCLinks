"""Config-driven model agents for candidate prediction and ranking picks."""

from __future__ import annotations

import hashlib
import importlib
import math
from abc import ABC, abstractmethod
from typing import Dict, Iterable, List

from tc_market.config_loader import load_model_configs
from tc_market.constants import MAX_PICKS_PER_CYCLE
from tc_market.market import MarketService
from tc_market.models import CandidateLink, ModelAgentConfig
from tc_market.storage import Storage


class ModelStrategy(ABC):
    @abstractmethod
    def predict_probabilities(
        self, config: ModelAgentConfig, candidates: List[CandidateLink]
    ) -> Dict[str, float]:
        raise NotImplementedError

    @abstractmethod
    def explain_choice(
        self,
        config: ModelAgentConfig,
        candidate: CandidateLink,
        probability: float,
        selected: bool,
    ) -> str:
        raise NotImplementedError


class DefaultRankingStrategy(ModelStrategy):
    """Deterministic baseline strategy based on URL hash and domain priors."""

    DOMAIN_BONUS = {
        "ft.com": 1.15,
        "economist.com": 1.12,
        "bloomberg.com": 1.08,
        "substack.com": 1.05,
        "arxiv.org": 1.1,
    }

    def predict_probabilities(
        self, config: ModelAgentConfig, candidates: List[CandidateLink]
    ) -> Dict[str, float]:
        raw_scores: Dict[str, float] = {}
        for candidate in candidates:
            digest = hashlib.sha256(f"{config.id}:{candidate.canonical_url}".encode("utf-8")).hexdigest()
            base = int(digest[:10], 16) / float(16**10)
            bonus = self.DOMAIN_BONUS.get(candidate.domain, 1.0)
            raw_scores[candidate.id] = max(0.0001, (0.5 + base) * bonus)

        total = sum(raw_scores.values())
        if total <= 0:
            uniform = 1.0 / len(candidates)
            return {candidate.id: uniform for candidate in candidates}

        return {candidate_id: score / total for candidate_id, score in raw_scores.items()}

    def explain_choice(
        self,
        config: ModelAgentConfig,
        candidate: CandidateLink,
        probability: float,
        selected: bool,
    ) -> str:
        rounded = f"{probability:.3f}"
        if selected:
            return (
                f"{config.model_name} selected this link because it scores well on likely assorted-links fit "
                f"(domain relevance plus novelty signal). Assigned probability: {rounded}."
            )
        return (
            f"{config.model_name} evaluated this link but ranked it below the top {config.max_daily_picks}. "
            f"Assigned probability: {rounded}."
        )


def _normalize_probabilities(
    probs: Dict[str, float],
    candidates: List[CandidateLink],
) -> Dict[str, float]:
    for candidate in candidates:
        probs.setdefault(candidate.id, 0.0)

    safe_scores = {key: max(0.0, value) for key, value in probs.items()}
    total = sum(safe_scores.values())
    if total <= 0:
        uniform = 1.0 / len(candidates) if candidates else 0.0
        return {candidate.id: uniform for candidate in candidates}

    return {candidate.id: safe_scores.get(candidate.id, 0.0) / total for candidate in candidates}


class ModelRunner:
    def __init__(self, storage: Storage, market: MarketService, config_path: str) -> None:
        self.storage = storage
        self.market = market
        self.config_path = config_path
        self.configs = load_model_configs(config_path)

    def reload_configs(self) -> List[ModelAgentConfig]:
        self.configs = load_model_configs(self.config_path)
        return self.configs

    def _load_strategy(self, config: ModelAgentConfig) -> ModelStrategy:
        if not config.strategy_plugin:
            return DefaultRankingStrategy()

        if ":" not in config.strategy_plugin:
            raise ValueError("strategy_plugin must be module:Class")

        module_name, class_name = config.strategy_plugin.split(":", 1)
        module = importlib.import_module(module_name)
        strategy_cls = getattr(module, class_name)
        strategy = strategy_cls()
        if not isinstance(strategy, ModelStrategy):
            raise TypeError("Plugin strategy must inherit ModelStrategy")
        return strategy

    def run_cycle(self, cycle_id: str) -> Dict[str, Dict[str, object]]:
        candidates = self.storage.list_candidates(cycle_id)
        if not candidates:
            return {}

        output: Dict[str, Dict[str, object]] = {}

        for config in self.configs:
            if not config.enabled:
                continue

            model_user = self.storage.get_or_create_ai_user(config.id)
            strategy = self._load_strategy(config)

            probabilities = strategy.predict_probabilities(config, candidates)
            probabilities = _normalize_probabilities(probabilities, candidates)

            ranked_candidates = sorted(
                candidates,
                key=lambda candidate: probabilities[candidate.id],
                reverse=True,
            )

            pick_cap = max(0, min(config.max_daily_picks, MAX_PICKS_PER_CYCLE, len(ranked_candidates)))
            selected_candidates = ranked_candidates[:pick_cap]
            selected_ids = [candidate.id for candidate in selected_candidates]
            selected_id_set = set(selected_ids)

            self.market.set_ranked_picks(cycle_id, model_user.id, selected_ids)

            model_rows = []
            for candidate in candidates:
                selected = candidate.id in selected_id_set
                explanation = strategy.explain_choice(
                    config,
                    candidate,
                    probabilities[candidate.id],
                    selected,
                )

                if selected and not explanation.strip():
                    raise ValueError(
                        f"Model {config.id} must provide explanation for selected candidate {candidate.id}"
                    )

                self.storage.upsert_model_prediction(
                    cycle_id=cycle_id,
                    model_user_id=model_user.id,
                    candidate_id=candidate.id,
                    probability=probabilities[candidate.id],
                    explanation=explanation,
                )
                model_rows.append(
                    {
                        "candidate_id": candidate.id,
                        "probability": probabilities[candidate.id],
                        "explanation": explanation,
                        "selected": selected,
                    }
                )

            model_rows.sort(key=lambda row: row["probability"], reverse=True)
            output[config.id] = {
                "model_user_id": model_user.id,
                "selected_count": len(selected_ids),
                "predictions": model_rows,
            }

        return output
