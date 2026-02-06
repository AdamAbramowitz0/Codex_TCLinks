"""Background job orchestration helpers."""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Dict, Optional

from tc_market.ingest import MarginalRevolutionIngestor
from tc_market.market import MarketService
from tc_market.model_agents import ModelRunner
from tc_market.storage import Storage


class JobService:
    def __init__(
        self,
        storage: Storage,
        market: MarketService,
        model_runner: ModelRunner,
        ingestor: Optional[MarginalRevolutionIngestor] = None,
    ) -> None:
        self.storage = storage
        self.market = market
        self.model_runner = model_runner
        self.ingestor = ingestor or MarginalRevolutionIngestor()

    def run_daily_faucet(self, as_of_date: Optional[str] = None, force: bool = False) -> Dict[str, object]:
        run_date = as_of_date or date.today().isoformat()
        run_key = run_date
        if not force and not self.storage.claim_job_run("daily_faucet", run_key, {"as_of_date": run_date}):
            return {"skipped": True, "reason": "already_ran", "run_key": run_key}

        credited = self.storage.apply_daily_faucet(run_date)
        return {"skipped": False, "run_key": run_key, "credited": credited}

    def run_models(self, cycle_id: Optional[str] = None, force: bool = False) -> Dict[str, object]:
        cycle = self.storage.get_cycle(cycle_id) if cycle_id else self.storage.get_open_cycle()
        if cycle is None:
            return {"skipped": True, "reason": "no_open_cycle"}

        run_key = f"{cycle.id}:{datetime.now(timezone.utc).strftime('%Y%m%d%H')}"
        if not force and not self.storage.claim_job_run("model_run", run_key, {"cycle_id": cycle.id}):
            return {"skipped": True, "reason": "already_ran", "run_key": run_key}

        result = self.model_runner.run_cycle(cycle.id)
        return {"skipped": False, "run_key": run_key, "cycle_id": cycle.id, "result": result}

    def sync_assorted_links(self, force: bool = False) -> Dict[str, object]:
        run_key = datetime.now(timezone.utc).strftime("%Y%m%d%H")
        if not force and not self.storage.claim_job_run("sync_assorted_links", run_key):
            return {"skipped": True, "reason": "already_ran", "run_key": run_key}

        return {"skipped": False, "run_key": run_key, **self.ingestor.sync(self.storage, self.market)}

    def run_curation_rewards(
        self,
        cycle_id: Optional[str] = None,
        force: bool = False,
        min_age_hours: int = 24,
    ) -> Dict[str, object]:
        targets = []
        if cycle_id:
            targets = [self.storage.get_cycle(cycle_id)]
        else:
            targets = [cycle for cycle in self.storage.list_cycles(200) if cycle.status == "SETTLED"]

        output = []
        for cycle in targets:
            run_key = f"{cycle.id}"
            if not force and not self.storage.claim_job_run("curation_rewards", run_key):
                output.append({"cycle_id": cycle.id, "skipped": True, "reason": "already_ran"})
                continue

            result = self.market.apply_curation_rewards(cycle.id, min_age_hours=min_age_hours, force=force)
            output.append({"cycle_id": cycle.id, **result})

        return {"results": output, "count": len(output)}
