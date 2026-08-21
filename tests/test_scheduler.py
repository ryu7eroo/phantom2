import asyncio

from ctf_swarm.agent import AgentContext
from ctf_swarm.models import Candidate, Challenge, Evidence
from ctf_swarm.scheduler import Scheduler


class SlowWrong:
    id = "slow-wrong"

    async def solve(self, context: AgentContext):
        await asyncio.sleep(0.05)
        return Evidence(self.id, context.challenge.id, "wrong path", failed_paths=("sql-union",))


class FastCorrect:
    id = "fast-correct"

    async def solve(self, context: AgentContext):
        await asyncio.sleep(0.01)
        return Candidate(self.id, context.challenge.id, "LISA{demo}", proof=("reproduced",), confidence=0.99)


class Noisy:
    id = "noisy"

    async def solve(self, context: AgentContext):
        await asyncio.sleep(0.02)
        return Evidence(self.id, context.challenge.id, "other", failed_paths=("path-a",))


def test_first_verified_candidate_wins_and_cancels():
    async def run():
        scheduler = Scheduler()
        challenge = Challenge("demo", "Demo", "misc", 100)
        result = await scheduler.solve(challenge, [SlowWrong(), FastCorrect(), Noisy()])
        assert result == "LISA{demo}"
        assert any(e.type.value == "global_cancel" for e in scheduler.events)

    asyncio.run(run())
