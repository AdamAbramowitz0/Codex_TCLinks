from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from tc_market.market import MarketService
from tc_market.model_agents import ModelRunner
from tc_market.storage import Storage


class ModelRunnerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.storage = Storage(":memory:")
        self.market = MarketService(self.storage)
        self.user = self.storage.create_user("Owner", "owner@example.com")
        self.cycle = self.storage.create_cycle("2026-02-06")

        self.storage.create_candidate(self.cycle.id, self.user.id, "https://a.com/1", "A")
        self.storage.create_candidate(self.cycle.id, self.user.id, "https://b.com/2", "B")
        self.storage.create_candidate(self.cycle.id, self.user.id, "https://c.com/3", "C")

    def tearDown(self) -> None:
        self.storage.close()

    def test_model_run_generates_probabilities_and_explanations(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "model_agents.yaml"
            config_path.write_text(
                """
models:
  - id: gpt-5.2
    provider: openai
    model_name: gpt-5.2
    enabled: true
    strategy_profile: default
    max_daily_picks: 10
    temperature: 0.2
                """.strip(),
                encoding="utf-8",
            )

            runner = ModelRunner(self.storage, self.market, str(config_path))
            result = runner.run_cycle(self.cycle.id)

            self.assertIn("gpt-5.2", result)
            rows = result["gpt-5.2"]["predictions"]
            self.assertTrue(rows)

            selected_rows = [row for row in rows if row["selected"]]
            self.assertLessEqual(len(selected_rows), 10)
            for row in selected_rows:
                self.assertTrue(row["explanation"].strip())

            total_probability = sum(row["probability"] for row in rows)
            self.assertAlmostEqual(total_probability, 1.0, places=6)

    def test_add_model_by_config_reload(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "model_agents.yaml"
            config_path.write_text(
                """
models:
  - id: gpt-5.2
    provider: openai
    model_name: gpt-5.2
    enabled: true
    strategy_profile: default
    max_daily_picks: 10
    temperature: 0.2
                """.strip(),
                encoding="utf-8",
            )

            runner = ModelRunner(self.storage, self.market, str(config_path))
            self.assertEqual(len(runner.configs), 1)

            config_path.write_text(
                """
models:
  - id: gpt-5.2
    provider: openai
    model_name: gpt-5.2
    enabled: true
    strategy_profile: default
    max_daily_picks: 10
    temperature: 0.2
  - id: gpt-5.1
    provider: openai
    model_name: gpt-5.1
    enabled: true
    strategy_profile: default
    max_daily_picks: 10
    temperature: 0.2
                """.strip(),
                encoding="utf-8",
            )
            configs = runner.reload_configs()
            self.assertEqual({cfg.id for cfg in configs}, {"gpt-5.2", "gpt-5.1"})

    def test_selected_predictions_require_explanation(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "model_agents.yaml"
            config_path.write_text(
                """
models:
  - id: bad-model
    provider: local
    model_name: bad
    enabled: true
    strategy_profile: default
    max_daily_picks: 2
    temperature: 0.2
    strategy_plugin: tests.plugins:BadExplanationStrategy
                """.strip(),
                encoding="utf-8",
            )

            runner = ModelRunner(self.storage, self.market, str(config_path))
            with self.assertRaises(ValueError):
                runner.run_cycle(self.cycle.id)


if __name__ == "__main__":
    unittest.main()
