from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from .models import Candidate, Challenge, Evidence, Round


@dataclass(frozen=True)
class AgentContext:
    challenge: Challenge
    round: Round
    dead_ends: frozenset[str]
    explored_paths: frozenset[str]
    open_hypotheses: frozenset[str]
    evidence: tuple[Evidence, ...]

    def must_diverge_from(self) -> frozenset[str]:
        """Paths an agent must avoid during divergent/adversarial rounds."""
        return self.dead_ends | self.explored_paths


class Agent(Protocol):
    id: str

    async def solve(self, context: AgentContext) -> Candidate | Evidence | None: ...
