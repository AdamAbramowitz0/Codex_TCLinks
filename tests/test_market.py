from __future__ import annotations

import unittest

from tc_market.constants import DAILY_CHIPS, MAX_PICKS_PER_CYCLE
from tc_market.market import MarketService
from tc_market.storage import Storage


class MarketRulesTests(unittest.TestCase):
    def setUp(self) -> None:
        self.storage = Storage(":memory:")
        self.market = MarketService(self.storage)

        self.user_a = self.storage.create_user("Alice", "alice@example.com")
        self.user_b = self.storage.create_user("Bob", "bob@example.com")
        self.cycle = self.storage.create_cycle("2026-02-06")

        self.c1 = self.storage.create_candidate(
            self.cycle.id, self.user_a.id, "https://example.com/a?utm_source=x", "A"
        )
        self.c2 = self.storage.create_candidate(self.cycle.id, self.user_a.id, "https://example.com/b", "B")
        self.c3 = self.storage.create_candidate(self.cycle.id, self.user_b.id, "https://example.com/c", "C")

    def tearDown(self) -> None:
        self.storage.close()

    def test_max_picks_enforced(self) -> None:
        extra_candidates = []
        for idx in range(4, 15):
            extra_candidates.append(
                self.storage.create_candidate(
                    self.cycle.id,
                    self.user_a.id,
                    f"https://example.com/{idx}",
                    f"{idx}",
                )
            )
        with self.assertRaises(ValueError):
            self.market.set_ranked_picks(
                self.cycle.id,
                self.user_a.id,
                [c.id for c in [self.c1, self.c2, self.c3, *extra_candidates]],
            )

    def test_wrong_picks_have_no_loss_and_correct_rank_gets_reward(self) -> None:
        self.market.set_ranked_picks(self.cycle.id, self.user_a.id, [self.c1.id, self.c2.id, self.c3.id])

        before = self.storage.get_user(self.user_a.id)
        self.assertEqual(before.current_chips, 100)

        # Tyler only linked candidate 1.
        result = self.market.settle_cycle(self.cycle.id, [self.c1.original_url])
        after = self.storage.get_user(self.user_a.id)

        # Rank 1 reward is +20, no penalty on wrong ranks 2 and 3.
        self.assertEqual(after.current_chips, 120)
        self.assertEqual(result["reward_model"]["wrong_pick_penalty"], 0)

    def test_market_probability_uses_rank_weights(self) -> None:
        # Alice: c1 rank 1, c2 rank 2
        self.market.set_ranked_picks(self.cycle.id, self.user_a.id, [self.c1.id, self.c2.id])
        # Bob: c1 rank 1, c3 rank 2
        self.market.set_ranked_picks(self.cycle.id, self.user_b.id, [self.c1.id, self.c3.id])

        probs = self.market.compute_market_probabilities(self.cycle.id)
        by_id = {row["candidate_id"]: row for row in probs}

        # weights: c1 = 10 + 10 = 20, c2 = 9, c3 = 9, total = 38
        self.assertAlmostEqual(by_id[self.c1.id]["market_probability"], 20 / 38, places=6)
        self.assertAlmostEqual(by_id[self.c2.id]["market_probability"], 9 / 38, places=6)
        self.assertAlmostEqual(by_id[self.c3.id]["market_probability"], 9 / 38, places=6)

    def test_daily_faucet_accumulates(self) -> None:
        credited = self.storage.apply_daily_faucet("2026-02-09")
        self.assertEqual(credited[self.user_a.id], DAILY_CHIPS * 3)
        self.assertEqual(credited[self.user_b.id], DAILY_CHIPS * 3)

        a = self.storage.get_user(self.user_a.id)
        self.assertEqual(a.current_chips, 100 + DAILY_CHIPS * 3)

    def test_same_domain_different_paths_are_distinct_candidates(self) -> None:
        c4 = self.storage.create_candidate(
            self.cycle.id,
            self.user_a.id,
            "https://news.site.com/alpha?utm_campaign=test",
            "Alpha",
        )
        c5 = self.storage.create_candidate(
            self.cycle.id,
            self.user_a.id,
            "https://news.site.com/beta",
            "Beta",
        )
        self.assertNotEqual(c4.id, c5.id)

        # Same canonical link should dedupe.
        c6 = self.storage.create_candidate(
            self.cycle.id,
            self.user_b.id,
            "https://news.site.com/alpha?utm_source=abc",
            "Alpha Dup",
        )
        self.assertEqual(c4.id, c6.id)


if __name__ == "__main__":
    unittest.main()
