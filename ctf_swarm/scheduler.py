from __future__ import annotations

import asyncio
from dataclasses import dataclass

from .agent import Agent, AgentContext
from .blackboard import Blackboard
from .models import Candidate, Challenge, EventType, Evidence, Round, TaskState
from .verifier import Verifier


@dataclass(frozen=True)
class Event:
    type: EventType
    challenge_id: str
    agent_id: str | None = None
    payload: object | None = None


class Scheduler:
    """Competitive multi-round orchestration with shared evidence and forced divergence."""

    def __init__(self, verifier: Verifier | None = None) -> None:
        self.verifier = verifier or Verifier()
        self.events: list[Event] = []

    async def run_round(self, challenge: Challenge, agents: list[Agent], state: TaskState) -> str | None:
        if state.cancelled or state.is_solved():
            return state.solved_value

        board = Blackboard(state)
        state.active_agents = {a.id for a in agents}
        self.events.append(Event(EventType.TASK_STARTED, challenge.id))

        async def run_one(agent: Agent) -> Candidate | Evidence | None:
            ctx = AgentContext(
                challenge=challenge,
                round=state.round,
                dead_ends=frozenset(state.dead_ends),
                explored_paths=frozenset(state.explored_paths),
                open_hypotheses=frozenset(board.available_hypotheses()),
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
                        state.solved_by = result.agent_id
                        state.cancelled = True
                        self.events.append(Event(EventType.VERIFIED, challenge.id, result.agent_id, result))
                        self.events.append(Event(EventType.GLOBAL_CANCEL, challenge.id, result.agent_id))
                        return result.value
                elif isinstance(result, Evidence):
                    board.add_evidence(result)
                    self.events.append(Event(EventType.EVIDENCE_MERGED, challenge.id, result.agent_id, result))
                    self.events.append(Event(EventType.HYPOTHESIS, challenge.id, result.agent_id, board.context()))
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            state.active_agents.clear()

        return None

    async def run_critic_round(self, challenge: Challenge, critic: Agent, state: TaskState) -> str | None:
        if state.cancelled or state.is_solved():
            return state.solved_value
        board = Blackboard(state)
        state.round = Round.ADVERSARIAL
        self.events.append(Event(EventType.ROUND_ADVANCED, challenge.id, payload=state.round.value))
        result = await critic.solve(
            AgentContext(
                challenge=challenge,
                round=Round.ADVERSARIAL,
                dead_ends=frozenset(state.dead_ends),
                explored_paths=frozenset(state.explored_paths),
                open_hypotheses=frozenset(board.available_hypotheses()),
                evidence=tuple(state.evidence),
            )
        )

        # A critic's output is always recorded as a critique before we decide
        # whether it constitutes a verified candidate. This keeps the event
        # stream auditable and guarantees the critic's shared-context work is
        # visible even when it immediately finds the solution.
        self.events.append(Event(EventType.CRITIQUE, challenge.id, critic.id, result))

        if isinstance(result, Candidate):
            state.candidates.append(result)
            if await self.verifier.verify(result):
                state.solved_value = result.value
                state.solved_by = result.agent_id
                state.cancelled = True
                self.events.append(Event(EventType.VERIFIED, challenge.id, result.agent_id, result))
                self.events.append(Event(EventType.GLOBAL_CANCEL, challenge.id, result.agent_id))
                return result.value
        elif isinstance(result, Evidence):
            board.add_evidence(result)
        return None

    async def solve(
        self,
        challenge: Challenge,
        agents: list[Agent],
        max_rounds: int = 4,
        critic: Agent | None = None,
    ) -> str | None:
        state = TaskState(challenge=challenge)
        rounds = [Round.INDEPENDENT, Round.COLLABORATIVE, Round.DIVERGENT]
        for round_name in rounds[:max_rounds]:
            state.round = round_name
            self.events.append(Event(EventType.ROUND_ADVANCED, challenge.id, payload=round_name.value))
            result = await self.run_round(challenge, agents, state)
            if result:
                return result

        if critic is not None and not state.is_solved():
            result = await self.run_critic_round(challenge, critic, state)
            if result:
                return result

        return None
