from __future__ import annotations

from typing import Dict, List

from tc_market.model_agents import ModelStrategy
from tc_market.models import CandidateLink, ModelAgentConfig


class BadExplanationStrategy(ModelStrategy):
    def predict_probabilities(
        self, config: ModelAgentConfig, candidates: List[CandidateLink]
    ) -> Dict[str, float]:
        if not candidates:
            return {}
        weight = 1.0 / len(candidates)
        return {candidate.id: weight for candidate in candidates}

    def explain_choice(
        self,
        config: ModelAgentConfig,
        candidate: CandidateLink,
        probability: float,
        selected: bool,
    ) -> str:
        if selected:
            return ""
        return "not selected"
