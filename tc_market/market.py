"""Core market operations and settlement logic."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List

from tc_market.constants import MAX_PICKS_PER_CYCLE, RANK_REWARDS, RANK_WEIGHTS
from tc_market.models import SettlementEntry
from tc_market.storage import Storage
from tc_market.url_utils import canonicalize_url


class MarketService:
    def __init__(self, storage: Storage) -> None:
        self.storage = storage

    def submit_candidate(self, cycle_id: str, user_id: str, url: str, title: str = "") -> Dict[str, Any]:
        candidate = self.storage.create_candidate(cycle_id, user_id, url, title)
        return {
            "id": candidate.id,
            "url": candidate.original_url,
            "canonical_url": candidate.canonical_url,
            "domain": candidate.domain,
            "title": candidate.title,
        }

    def set_ranked_picks(self, cycle_id: str, user_id: str, candidate_ids: Iterable[str]) -> List[Dict[str, Any]]:
        ranked_ids = list(candidate_ids)
        if len(ranked_ids) > MAX_PICKS_PER_CYCLE:
            raise ValueError(f"At most {MAX_PICKS_PER_CYCLE} picks are allowed")

        picks = self.storage.set_ranked_picks(cycle_id, user_id, ranked_ids)
        return [
            {
                "candidate_id": pick.candidate_id,
                "rank": pick.rank,
                "picked_at": pick.picked_at,
            }
            for pick in picks
        ]

    def compute_market_probabilities(self, cycle_id: str) -> List[Dict[str, Any]]:
        candidates = self.storage.list_candidates(cycle_id)
        picks = self.storage.list_picks(cycle_id)

        weights: Dict[str, int] = defaultdict(int)
        for pick in picks:
            weights[pick.candidate_id] += RANK_WEIGHTS.get(pick.rank, 1)

        total_weight = sum(weights.values())

        output: List[Dict[str, Any]] = []
        for candidate in candidates:
            score = weights.get(candidate.id, 0)
            probability = (score / total_weight) if total_weight > 0 else 0.0
            output.append(
                {
                    "candidate_id": candidate.id,
                    "url": candidate.original_url,
                    "domain": candidate.domain,
                    "rank_weight_score": score,
                    "market_probability": probability,
                }
            )

        output.sort(key=lambda item: item["market_probability"], reverse=True)
        return output

    def settle_cycle(self, cycle_id: str, winner_urls: Iterable[str]) -> Dict[str, Any]:
        candidates = self.storage.list_candidates(cycle_id)
        candidate_by_canonical = {candidate.canonical_url: candidate for candidate in candidates}

        winner_canonical_urls = {canonicalize_url(url) for url in winner_urls}
        winner_candidate_ids = []
        for canonical in winner_canonical_urls:
            candidate = candidate_by_canonical.get(canonical)
            if candidate:
                winner_candidate_ids.append(candidate.id)

        winner_set = set(winner_candidate_ids)
        self.storage.save_cycle_results(cycle_id, winner_candidate_ids)

        picks = self.storage.list_picks(cycle_id)
        user_rewards: Dict[str, int] = defaultdict(int)
        user_hits: Dict[str, int] = defaultdict(int)
        participant_ids = []
        seen_users = set()

        for pick in picks:
            if pick.user_id not in seen_users:
                participant_ids.append(pick.user_id)
                seen_users.add(pick.user_id)

            if pick.candidate_id in winner_set:
                reward = RANK_REWARDS.get(pick.rank, 0)
                user_rewards[pick.user_id] += reward
                user_hits[pick.user_id] += 1

        for user_id, reward in user_rewards.items():
            if reward <= 0:
                continue
            self.storage.credit_user_chips(
                user_id=user_id,
                chips_delta=reward,
                event_type="prediction_reward",
                cycle_id=cycle_id,
                metadata={"correct_picks": user_hits[user_id]},
            )

        ranking_entries = []
        for user_id in participant_ids:
            ranking_entries.append(
                {
                    "user_id": user_id,
                    "correct_count": user_hits.get(user_id, 0),
                    "reward_chips": user_rewards.get(user_id, 0),
                }
            )

        ranking_entries.sort(key=lambda x: (x["reward_chips"], x["correct_count"]), reverse=True)

        entries_with_ranks: List[SettlementEntry] = []
        current_rank = 0
        previous_score_key = None
        for idx, row in enumerate(ranking_entries, start=1):
            score_key = (row["reward_chips"], row["correct_count"])
            if score_key != previous_score_key:
                current_rank = idx
                previous_score_key = score_key
            entries_with_ranks.append(
                SettlementEntry(
                    user_id=row["user_id"],
                    correct_count=row["correct_count"],
                    reward_chips=row["reward_chips"],
                    rank=current_rank,
                )
            )

        return {
            "cycle_id": cycle_id,
            "winner_candidate_ids": winner_candidate_ids,
            "winner_count": len(winner_candidate_ids),
            "ranking": [entry.__dict__ for entry in entries_with_ranks],
            "reward_model": {
                "wrong_pick_penalty": 0,
                "max_picks": MAX_PICKS_PER_CYCLE,
                "rank_rewards": RANK_REWARDS,
            },
        }

    def apply_curation_rewards(
        self,
        cycle_id: str,
        min_age_hours: int = 24,
        force: bool = False,
    ) -> Dict[str, Any]:
        cycle = self.storage.get_cycle(cycle_id)
        if cycle.status != "SETTLED":
            return {"awarded": False, "reason": "cycle_not_settled", "rows": []}

        if not force and cycle.closed_at:
            closed_at = cycle.closed_at
            if closed_at.endswith("Z"):
                closed_at = closed_at.replace("Z", "+00:00")
            closed_at_dt = datetime.fromisoformat(closed_at)
            if datetime.now(timezone.utc) - closed_at_dt < timedelta(hours=min_age_hours):
                return {"awarded": False, "reason": "wait_window", "rows": []}

        rows = self.storage.apply_curation_rewards(cycle_id)
        if not rows:
            return {"awarded": False, "reason": "none_or_already_awarded", "rows": []}

        return {"awarded": True, "reason": "ok", "rows": rows}
