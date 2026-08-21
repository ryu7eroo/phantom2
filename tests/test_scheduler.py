import asyncio

from ctf_swarm.agent import AgentContext
from ctf_swarm.models import Candidate, Challenge, Evidence, Round
from ctf_swarm.scheduler import Scheduler


class SlowWrong:
    id = "slow-wrong"

    async def solve(self, context: AgentContext):
        await asyncio.sleep(0.05)
        return Evidence(
            self.id,
            context.challenge.id,
            "wrong path",
            tested_paths=("sql-union",),
            failed_paths=("sql-union",),
            next_hypotheses=("heap-uaf",),
            round=context.round,
        )


class FastCorrect:
    id = "fast-correct"

    async def solve(self, context: AgentContext):
        await asyncio.sleep(0.01)
        return Candidate(self.id, context.challenge.id, "LISA{demo}", proof=("reproduced",), confidence=0.99)


class Noisy:
    id = "noisy"

    async def solve(self, context: AgentContext):
        await asyncio.sleep(0.02)
        return Evidence(
            self.id,
            context.challenge.id,
            "other",
            tested_paths=("path-a",),
            failed_paths=("path-a",),
            round=context.round,
        )


class RoundAwareAgent:
    def __init__(self, agent_id: str, path: str, solve_on: Round | None = None):
        self.id = agent_id
        self.path = path
        self.solve_on = solve_on
        self.seen: list[AgentContext] = []

    async def solve(self, context: AgentContext):
        self.seen.append(context)
        if self.solve_on == context.round:
            return Candidate(self.id, context.challenge.id, "LISA{round-win}", proof=("round-proof",))
        return Evidence(
            self.id,
            context.challenge.id,
            f"explored {self.path}",
            tested_paths=(self.path,),
            failed_paths=(self.path,),
            next_hypotheses=("new-branch",) if context.round == Round.INDEPENDENT else (),
            round=context.round,
        )


class Critic:
    id = "critic"

    async def solve(self, context: AgentContext):
        assert context.round == Round.ADVERSARIAL
        assert "sql-union" in context.dead_ends
        return Candidate(self.id, context.challenge.id, "LISA{critic-win}", proof=("counterexample",))


def test_first_verified_candidate_wins_and_cancels():
    async def run():
        scheduler = Scheduler()
        challenge = Challenge("demo", "Demo", "misc", 100)
        result = await scheduler.solve(challenge, [SlowWrong(), FastCorrect(), Noisy()])
        assert result == "LISA{demo}"
        assert any(e.type.value == "global_cancel" for e in scheduler.events)

    asyncio.run(run())


def test_failed_round_merges_evidence_and_next_round_sees_dead_ends():
    async def run():
        scheduler = Scheduler()
        challenge = Challenge("rounds", "Rounds", "misc", 100)
        a = RoundAwareAgent("a", "sql-union", solve_on=Round.DIVERGENT)
        b = RoundAwareAgent("b", "format-string")
        result = await scheduler.solve(challenge, [a, b], max_rounds=3)
        assert result == "LISA{round-win}"
        assert a.seen[0].round == Round.INDEPENDENT
        assert a.seen[-1].round == Round.DIVERGENT
        assert "sql-union" in a.seen[-1].dead_ends
        assert any(e.type.value == "evidence_merged" for e in scheduler.events)

    asyncio.run(run())


def test_critic_gets_shared_evidence_after_failed_rounds():
    async def run():
        scheduler = Scheduler()
        challenge = Challenge("critic", "Critic", "misc", 100)
        result = await scheduler.solve(challenge, [SlowWrong()], max_rounds=1, critic=Critic())
        assert result == "LISA{critic-win}"
        assert any(e.type.value == "critique" for e in scheduler.events)

    asyncio.run(run())
