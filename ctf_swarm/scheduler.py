from __future__ import annotations

import asyncio
from dataclasses import dataclass

from .agent import Agent, AgentContext
from .blackboard import Blackboard
from .models import Candidate, Challenge, EventType, Round, TaskState
from .verifier import Verifier


@dataclass(frozen=True)
class Event:
    type: EventType
    challenge_id: str
    agent_id: str | None = None
    payload: object | None = None


class Scheduler:
    """Deterministic V1 orchestration core.

    Provider/model execution is intentionally injected through Agent objects.
    """

    def __init__(self, verifier: Verifier | None = None) -> None:
        self.verifier = verifier or Verifier()
        self.events: list[Event] = []

    async def run_round(self, challenge: Challenge, agents: list[Agent], state: TaskState) -> str | None:
        if state.cancelled or state.is_solved():
            return state.solved_value

        state.active_agents = {a.id for a in agents}
        self.events.append(Event(EventType.TASK_STARTED, challenge.id))
        board = Blackboard(state)

        async def run_one(agent: Agent) -> Candidate | object | None:
            ctx = AgentContext(
                challenge=challenge,
                round=state.round,
                dead_ends=frozenset(state.dead_ends),
                evidence=tuple(state.evidence),
            )
            return await agent.solve(ctx)

        tasks = {asyncio.create_task(run_one(a)): a.id for a in agents}
        try:
            for task in asyncio.as_completed(tasks):
                result = await task
                if isinstance(result, Candidate):
                    state.candidates.append(result)
                    self.events.append(Event(EventType.CANDIDATE, challenge.id, result.agent_id, result))
                    if await self.verifier.verify(result):
                        state.solved_value = result.value
                        state.cancelled = True
                        self.events.append(Event(EventType.VERIFIED, challenge.id, result.agent_id, result))
                        self.events.append(Event(EventType.GLOBAL_CANCEL, challenge.id, result.agent_id))
                        return result.value
                elif result is not None:
                    board.add_evidence(result)
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            state.active_agents.clear()

        return None

    async def solve(self, challenge: Challenge, agents: list[Agent], max_rounds: int = 4) -> str | None:
        state = TaskState(challenge=challenge)
        rounds = [Round.INDEPENDENT, Round.COLLABORATIVE, Round.DIVERGENT, Round.ADVERSARIAL]
        for index in range(min(max_rounds, len(rounds))):
            state.round = rounds[index]
            self.events.append(Event(EventType.ROUND_ADVANCED, challenge.id, payload=state.round.value))
            result = await self.run_round(challenge, agents, state)
            if result:
                return result
        return None
