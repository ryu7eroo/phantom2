from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from .models import Candidate, Challenge, Evidence, Round


@dataclass(frozen=True)
class AgentContext:
    challenge: Challenge
    round: Round
    dead_ends: frozenset[str]
    evidence: tuple[Evidence, ...]


class Agent(Protocol):
    id: str

    async def solve(self, context: AgentContext) -> Candidate | Evidence | None: ...
